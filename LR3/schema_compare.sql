-- 1. Создаем объекты для вывода результата (Record и Table Type)
CREATE OR REPLACE TYPE t_schema_comp_row AS OBJECT (
    table_name  VARCHAR2(128),
    status      VARCHAR2(50),
    details     CLOB,
    sort_order  NUMBER
);
/

CREATE OR REPLACE TYPE t_schema_comp_table AS TABLE OF t_schema_comp_row;
/

-- 2. Основная функция сравнения схем
CREATE OR REPLACE FUNCTION compare_schemas_oracle(
    p_dev_schema  IN VARCHAR2,
    p_prod_schema IN VARCHAR2
) RETURN t_schema_comp_table PIPELINED 
AS
    -- Коллекции для хранения состояний и графа
    TYPE t_tab_state IS TABLE OF NUMBER INDEX BY VARCHAR2(128); -- 0: не посещен, 1: в стеке, 2: обработан
    TYPE t_report_data IS TABLE OF VARCHAR2(4000) INDEX BY VARCHAR2(128);
    
    v_all_dev_tables   t_tab_state;
    v_tables_to_report t_report_data;
    v_sorted_indices   t_tab_state;
    
    v_sort_counter     NUMBER := 0;
    v_has_cycle        BOOLEAN := FALSE;
    v_cycle_msg        VARCHAR2(4000);

    -- Внутренняя рекурсивная процедура для DFS (сортировка + циклы)
    PROCEDURE visit_table(p_tn IN VARCHAR2, p_stack_path IN VARCHAR2 DEFAULT '') IS
        v_current_path VARCHAR2(4000);
    BEGIN
        IF v_has_cycle THEN RETURN; END IF;
        
        -- Обнаружение цикла (попали в "серую" вершину)
        IF v_all_dev_tables.EXISTS(p_tn) AND v_all_dev_tables(p_tn) = 1 THEN
            v_has_cycle := TRUE;
            v_cycle_msg := 'Circular dependency detected: ' || p_stack_path || ' -> ' || p_tn;
            RETURN;
        END IF;

        -- Если вершина еще "белая" (не посещена)
        IF v_all_dev_tables.EXISTS(p_tn) AND v_all_dev_tables(p_tn) = 0 THEN
            v_all_dev_tables(p_tn) := 1; -- Переводим в "серую" (в стеке)
            v_current_path := p_stack_path || (CASE WHEN p_stack_path IS NULL THEN '' ELSE ' -> ' END) || p_tn;

            -- Ищем все таблицы, на которые ссылается данная (Foreign Keys)
            FOR r IN (
                SELECT DISTINCT r.table_name as parent_table
                FROM all_constraints c
                JOIN all_constraints r ON c.r_constraint_name = r.constraint_name AND c.r_owner = r.owner
                WHERE c.owner = UPPER(p_dev_schema)
                  AND c.table_name = UPPER(p_tn)
                  AND c.constraint_type = 'R'
                  AND r.table_name <> c.table_name -- Пропускаем ссылки на самих себя
            ) LOOP
                visit_table(r.parent_table, v_current_path);
            END LOOP;

            v_all_dev_tables(p_tn) := 2; -- "Черная" (полностью обработана)
            v_sort_counter := v_sort_counter + 1;
            v_sorted_indices(p_tn) := v_sort_counter;
        END IF;
    END visit_table;

BEGIN
    -- [1] Инициализируем список всех таблиц из Dev
    FOR t IN (SELECT table_name FROM all_tables WHERE owner = UPPER(p_dev_schema)) LOOP
        v_all_dev_tables(t.table_name) := 0;
    END LOOP;

    -- [2] Сравниваем структуры (MINUS + Join)
    -- Находим таблицы, которых нет в Prod
    FOR r IN (
        SELECT table_name FROM all_tables WHERE owner = UPPER(p_dev_schema)
        MINUS
        SELECT table_name FROM all_tables WHERE owner = UPPER(p_prod_schema)
    ) LOOP
        v_tables_to_report(r.table_name) := 'Table missing in PROD';
    END LOOP;

    -- Находим таблицы с различиями в колонках (только тех, которых ещё нет в отчёте)
    FOR r IN (
        SELECT dev.table_name, 
               'Structure mismatch: ' || LISTAGG(dev.column_name || ' (' || dev.data_type || ')', ', ') 
               WITHIN GROUP (ORDER BY dev.column_id) as diff_info
        FROM all_tab_columns dev
        LEFT JOIN all_tab_columns prod 
          ON dev.table_name = prod.table_name 
         AND dev.column_name = prod.column_name
         AND prod.owner = UPPER(p_prod_schema)
        WHERE dev.owner = UPPER(p_dev_schema)
          AND (prod.column_name IS NULL 
               OR dev.data_type <> prod.data_type 
               OR dev.data_length <> prod.data_length
               OR dev.nullable <> prod.nullable)
        GROUP BY dev.table_name
    ) LOOP
        -- Не перезаписываем, если таблица уже в отчёте (missing)
        IF NOT v_tables_to_report.EXISTS(r.table_name) THEN
            v_tables_to_report(r.table_name) := r.diff_info;
        END IF;
    END LOOP;

    -- [3] Запускаем DFS для всех таблиц для определения порядка создания
    DECLARE
        v_idx VARCHAR2(128);
    BEGIN
        v_idx := v_all_dev_tables.FIRST;
        WHILE v_idx IS NOT NULL LOOP
            IF v_all_dev_tables(v_idx) = 0 THEN
                visit_table(v_idx);
            END IF;
            v_idx := v_all_dev_tables.NEXT(v_idx);
        END LOOP;
    END;

    -- [4] Формируем финальный результат
    IF v_has_cycle THEN
        PIPE ROW(t_schema_comp_row('!!! ERROR !!!', 'CIRCULAR', v_cycle_msg, NULL));
    ELSE
        -- Выводим только те таблицы, которые попали в отчет (различаются)
        DECLARE
            v_idx VARCHAR2(128);
        BEGIN
            v_idx := v_tables_to_report.FIRST;
            WHILE v_idx IS NOT NULL LOOP
                PIPE ROW(t_schema_comp_row(
                    v_idx, 
                    CASE WHEN v_tables_to_report(v_idx) LIKE 'Table missing%' THEN 'MISSING' ELSE 'DIFFERENT' END, 
                    v_tables_to_report(v_idx), 
                    v_sorted_indices(v_idx)
                ));
                v_idx := v_tables_to_report.NEXT(v_idx);
            END LOOP;
        END;
    END IF;

    RETURN;
END;
/

-- Инструкция по использованию:
-- 1. Выполните скрипт целиком для создания типов и функции.
-- 2. Используйте следующий SELECT для получения отчета:
--
SELECT * FROM TABLE(compare_schemas_oracle('DEV_USER', 'PROD_USER'))
ORDER BY sort_order;
