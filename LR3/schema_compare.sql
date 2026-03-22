-- ============================================================================
-- ЛАБОРАТОРНАЯ РАБОТА 3: Полное сравнение схем Oracle (Dev vs Prod)
-- ============================================================================
-- Задание 1: Сравнение таблиц с топологической сортировкой (DFS)
-- Задание 2: Сравнение процедур, функций, индексов, пакетов
-- Задание 3: Генерация DDL-скриптов для синхронизации
-- ============================================================================
DROP FUNCTION compare_schemas_oracle;
DROP TYPE t_schema_comp_table;
DROP TYPE t_schema_comp_row;
-- 1. Создаем объекты для вывода результата (Record и Table Type)
CREATE OR REPLACE TYPE t_schema_comp_row AS OBJECT (
    obj_type    VARCHAR2(50),   -- TABLE, PROCEDURE, FUNCTION, INDEX, PACKAGE
    obj_name    VARCHAR2(128),  -- Имя объекта
    status      VARCHAR2(50),   -- MISSING, DIFFERENT, EXTRA_IN_PROD
    details     CLOB,           -- Описание различий
    ddl_script  CLOB,           -- DDL скрипт для синхронизации
    sort_order  NUMBER          -- Порядок выполнения
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
    TYPE t_result_rec IS RECORD (
        obj_type    VARCHAR2(50),
        obj_name    VARCHAR2(128),
        status      VARCHAR2(50),
        details     CLOB,
        ddl_script  CLOB,
        sort_order  NUMBER
    );
    TYPE t_result_list IS TABLE OF t_result_rec INDEX BY PLS_INTEGER;
    
    v_all_dev_tables   t_tab_state;
    v_sorted_indices   t_tab_state;
    v_results          t_result_list;
    v_result_idx       PLS_INTEGER := 0;
    
    v_sort_counter     NUMBER := 0;
    v_has_cycle        BOOLEAN := FALSE;
    v_cycle_msg        VARCHAR2(4000);
    
    v_ddl              CLOB;
    v_details          CLOB;

    -- ========================================================================
    -- ЛОКАЛЬНЫЕ ФУНКЦИИ ДЛЯ ГЕНЕРАЦИИ DDL
    -- ========================================================================
    
    -- Генерация CREATE TABLE DDL
    FUNCTION generate_create_table_ddl(p_schema VARCHAR2, p_table_name VARCHAR2) RETURN CLOB IS
        v_ddl CLOB;
        v_first BOOLEAN := TRUE;
        v_pk_cols VARCHAR2(4000);
    BEGIN
        v_ddl := 'CREATE TABLE "' || p_table_name || '" (' || CHR(10);
        
        -- Добавляем колонки
        FOR col IN (
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM all_tab_columns
            WHERE owner = UPPER(p_schema) AND table_name = UPPER(p_table_name)
            ORDER BY column_id
        ) LOOP
            IF NOT v_first THEN
                v_ddl := v_ddl || ',' || CHR(10);
            END IF;
            v_first := FALSE;
            
            v_ddl := v_ddl || '  "' || col.column_name || '" ' || col.data_type;
            
            -- Добавляем размерность
            IF col.data_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR') THEN
                v_ddl := v_ddl || '(' || col.data_length || ')';
            ELSIF col.data_type = 'NUMBER' THEN
                IF col.data_precision IS NOT NULL THEN
                    v_ddl := v_ddl || '(' || col.data_precision;
                    IF col.data_scale IS NOT NULL AND col.data_scale > 0 THEN
                        v_ddl := v_ddl || ',' || col.data_scale;
                    END IF;
                    v_ddl := v_ddl || ')';
                END IF;
            END IF;
            
            -- NOT NULL
            IF col.nullable = 'N' THEN
                v_ddl := v_ddl || ' NOT NULL';
            END IF;
        END LOOP;
        
        -- Добавляем PRIMARY KEY (используем оригинальное имя constraint)
        DECLARE
            v_pk_constraint_name VARCHAR2(128);
        BEGIN
            SELECT c.constraint_name,
                   LISTAGG('"' || cc.column_name || '"', ', ') WITHIN GROUP (ORDER BY cc.position)
            INTO v_pk_constraint_name, v_pk_cols
            FROM all_constraints c
            JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner
            WHERE c.owner = UPPER(p_schema)
              AND c.table_name = UPPER(p_table_name)
              AND c.constraint_type = 'P'
            GROUP BY c.constraint_name;
            
            IF v_pk_cols IS NOT NULL THEN
                -- Если имя constraint начинается с SYS_ (генерированное Oracle), не указываем имя
                IF v_pk_constraint_name LIKE 'SYS_%' THEN
                    v_ddl := v_ddl || ',' || CHR(10) || '  PRIMARY KEY (' || v_pk_cols || ')';
                ELSE
                    v_ddl := v_ddl || ',' || CHR(10) || '  CONSTRAINT "' || v_pk_constraint_name || '" PRIMARY KEY (' || v_pk_cols || ')';
                END IF;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
        END;
        
        -- Добавляем FOREIGN KEY
        FOR fk IN (
            SELECT c.constraint_name,
                   (SELECT LISTAGG('"' || cc.column_name || '"', ', ') WITHIN GROUP (ORDER BY cc.position)
                    FROM all_cons_columns cc 
                    WHERE cc.constraint_name = c.constraint_name AND cc.owner = c.owner) as fk_cols,
                   (SELECT ref_c.table_name FROM all_constraints ref_c 
                    WHERE ref_c.constraint_name = c.r_constraint_name AND ref_c.owner = c.r_owner) as ref_table,
                   (SELECT LISTAGG('"' || rcc.column_name || '"', ', ') WITHIN GROUP (ORDER BY rcc.position)
                    FROM all_cons_columns rcc
                    JOIN all_constraints ref_c ON rcc.constraint_name = ref_c.constraint_name AND rcc.owner = ref_c.owner
                    WHERE ref_c.constraint_name = c.r_constraint_name AND ref_c.owner = c.r_owner) as ref_cols
            FROM all_constraints c
            WHERE c.owner = UPPER(p_schema)
              AND c.table_name = UPPER(p_table_name)
              AND c.constraint_type = 'R'
        ) LOOP
            v_ddl := v_ddl || ',' || CHR(10) || '  CONSTRAINT "' || fk.constraint_name || '" FOREIGN KEY (' || fk.fk_cols || ')' 
                    || ' REFERENCES "' || fk.ref_table || '" (' || fk.ref_cols || ')';
        END LOOP;
        
        v_ddl := v_ddl || CHR(10) || ');';
        RETURN v_ddl;
    END generate_create_table_ddl;
    
    -- Генерация ALTER TABLE ADD COLUMN DDL
    FUNCTION generate_alter_add_columns_ddl(
        p_schema VARCHAR2, 
        p_table_name VARCHAR2, 
        p_prod_schema VARCHAR2
    ) RETURN CLOB IS
        v_ddl CLOB := '';
        v_col_ddl VARCHAR2(4000);
    BEGIN
        -- Находим отсутствующие или измененные колонки
        FOR col IN (
            SELECT dev.column_name, dev.data_type, dev.data_length, dev.data_precision, 
                   dev.data_scale, dev.nullable
            FROM all_tab_columns dev
            LEFT JOIN all_tab_columns prod 
              ON dev.table_name = prod.table_name 
             AND dev.column_name = prod.column_name
             AND prod.owner = UPPER(p_prod_schema)
            WHERE dev.owner = UPPER(p_schema)
              AND dev.table_name = UPPER(p_table_name)
              AND (prod.column_name IS NULL 
                   OR dev.data_type <> prod.data_type 
                   OR dev.data_length <> prod.data_length
                   OR dev.nullable <> prod.nullable)
            ORDER BY dev.column_id
        ) LOOP
            v_col_ddl := 'ALTER TABLE "' || p_table_name || '" ADD "' || col.column_name || '" ' || col.data_type;
            
            -- Размерность
            IF col.data_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR') THEN
                v_col_ddl := v_col_ddl || '(' || col.data_length || ')';
            ELSIF col.data_type = 'NUMBER' THEN
                IF col.data_precision IS NOT NULL THEN
                    v_col_ddl := v_col_ddl || '(' || col.data_precision;
                    IF col.data_scale IS NOT NULL AND col.data_scale > 0 THEN
                        v_col_ddl := v_col_ddl || ',' || col.data_scale;
                    END IF;
                    v_col_ddl := v_col_ddl || ')';
                END IF;
            END IF;
            
            -- NOT NULL
            IF col.nullable = 'N' THEN
                v_col_ddl := v_col_ddl || ' NOT NULL';
            END IF;
            
            v_col_ddl := v_col_ddl || ';';
            
            IF v_ddl IS NOT NULL THEN
                v_ddl := v_ddl || CHR(10);
            END IF;
            v_ddl := v_ddl || v_col_ddl;
        END LOOP;
        
        RETURN v_ddl;
    END generate_alter_add_columns_ddl;
    
    -- Генерация DROP DDL
    FUNCTION generate_drop_ddl(p_obj_type VARCHAR2, p_obj_name VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        IF p_obj_type = 'TABLE' THEN
            RETURN 'DROP TABLE "' || p_obj_name || '" CASCADE CONSTRAINTS;';
        ELSIF p_obj_type = 'PROCEDURE' THEN
            RETURN 'DROP PROCEDURE "' || p_obj_name || '";';
        ELSIF p_obj_type = 'FUNCTION' THEN
            RETURN 'DROP FUNCTION "' || p_obj_name || '";';
        ELSIF p_obj_type = 'PACKAGE' THEN
            RETURN 'DROP PACKAGE "' || p_obj_name || '";';
        ELSIF p_obj_type = 'INDEX' THEN
            RETURN 'DROP INDEX "' || p_obj_name || '";';
        ELSE
            RETURN '-- Unknown object type: ' || p_obj_type;
        END IF;
    END generate_drop_ddl;
    
    -- Генерация CREATE INDEX DDL
    FUNCTION generate_create_index_ddl(p_schema VARCHAR2, p_index_name VARCHAR2) RETURN CLOB IS
        v_ddl CLOB;
        v_table_name VARCHAR2(128);
        v_uniqueness VARCHAR2(10);
        v_cols VARCHAR2(4000);
    BEGIN
        -- Получаем информацию об индексе
        SELECT table_name, uniqueness
        INTO v_table_name, v_uniqueness
        FROM all_indexes
        WHERE owner = UPPER(p_schema) AND index_name = UPPER(p_index_name);
        
        -- Получаем список колонок
        SELECT LISTAGG('"' || column_name || '"', ', ') WITHIN GROUP (ORDER BY column_position)
        INTO v_cols
        FROM all_ind_columns
        WHERE index_owner = UPPER(p_schema) AND index_name = UPPER(p_index_name);
        
        -- Формируем DDL
        v_ddl := 'CREATE ';
        IF v_uniqueness = 'UNIQUE' THEN
            v_ddl := v_ddl || 'UNIQUE ';
        END IF;
        v_ddl := v_ddl || 'INDEX "' || p_index_name || '" ON "' || v_table_name || '" (' || v_cols || ');';
        
        RETURN v_ddl;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN '-- Index "' || p_index_name || '" not found';
    END generate_create_index_ddl;
    
    -- Получение DDL через DBMS_METADATA (для процедур, функций, пакетов)
    FUNCTION get_ddl_via_metadata(p_obj_type VARCHAR2, p_obj_name VARCHAR2, p_schema VARCHAR2) RETURN CLOB IS
        v_ddl CLOB;
        v_schema_prefix VARCHAR2(128);
    BEGIN
        v_ddl := DBMS_METADATA.GET_DDL(p_obj_type, UPPER(p_obj_name), UPPER(p_schema));
        -- Убираем имя схемы из DDL (например: "DEV_USER"."PROC" -> "PROC")
        v_schema_prefix := '"' || UPPER(p_schema) || '".';
        v_ddl := REPLACE(v_ddl, v_schema_prefix, '');
        -- Также убираем EDITIONABLE если есть
        v_ddl := REPLACE(v_ddl, 'EDITIONABLE ', '');
        RETURN v_ddl;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN '-- Error getting DDL for ' || p_obj_type || ' "' || p_obj_name || '": ' || SQLERRM;
    END get_ddl_via_metadata;

    -- ========================================================================
    -- ВНУТРЕННЯЯ РЕКУРСИВНАЯ ПРОЦЕДУРА ДЛЯ DFS (ТОПОЛОГИЧЕСКАЯ СОРТИРОВКА)
    -- ========================================================================
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
    -- ========================================================================
    -- ЗАДАНИЕ 1: СРАВНЕНИЕ ТАБЛИЦ
    -- ========================================================================
    
    -- [1] Инициализируем список всех таблиц из Dev
    FOR t IN (SELECT table_name FROM all_tables WHERE owner = UPPER(p_dev_schema)) LOOP
        v_all_dev_tables(t.table_name) := 0;
    END LOOP;

    -- [2] Запускаем DFS для всех таблиц для определения порядка создания
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
    
    -- [3] Проверяем наличие циклов
    IF v_has_cycle THEN
        PIPE ROW(t_schema_comp_row('ERROR', '!!! CIRCULAR DEPENDENCY !!!', 'CRITICAL', v_cycle_msg, NULL, NULL));
        RETURN;
    END IF;

    -- [4] Сравниваем таблицы: MISSING в PROD
    FOR r IN (
        SELECT table_name FROM all_tables WHERE owner = UPPER(p_dev_schema)
        MINUS
        SELECT table_name FROM all_tables WHERE owner = UPPER(p_prod_schema)
    ) LOOP
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'TABLE';
        v_results(v_result_idx).obj_name := r.table_name;
        v_results(v_result_idx).status := 'MISSING';
        v_results(v_result_idx).details := 'Table missing in PROD schema';
        v_results(v_result_idx).ddl_script := generate_create_table_ddl(p_dev_schema, r.table_name);
        v_results(v_result_idx).sort_order := v_sorted_indices(r.table_name);
    END LOOP;

    -- [5] Сравниваем структуру таблиц: DIFFERENT
    FOR r IN (
        SELECT dev.table_name, 
               COUNT(DISTINCT dev.column_name) as diff_col_count
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
          AND dev.table_name IN (SELECT table_name FROM all_tables WHERE owner = UPPER(p_prod_schema))
        GROUP BY dev.table_name
    ) LOOP
        -- Формируем детальное описание различий
        SELECT 'Structure mismatch: ' || LISTAGG(dev.column_name || ' (' || dev.data_type || ')', ', ') 
               WITHIN GROUP (ORDER BY dev.column_id)
        INTO v_details
        FROM all_tab_columns dev
        LEFT JOIN all_tab_columns prod 
          ON dev.table_name = prod.table_name 
         AND dev.column_name = prod.column_name
         AND prod.owner = UPPER(p_prod_schema)
        WHERE dev.owner = UPPER(p_dev_schema)
          AND dev.table_name = r.table_name
          AND (prod.column_name IS NULL 
               OR dev.data_type <> prod.data_type 
               OR dev.data_length <> prod.data_length
               OR dev.nullable <> prod.nullable);
        
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'TABLE';
        v_results(v_result_idx).obj_name := r.table_name;
        v_results(v_result_idx).status := 'DIFFERENT';
        v_results(v_result_idx).details := v_details;
        v_results(v_result_idx).ddl_script := generate_alter_add_columns_ddl(p_dev_schema, r.table_name, p_prod_schema);
        v_results(v_result_idx).sort_order := v_sorted_indices(r.table_name);
    END LOOP;

    -- [6] Находим EXTRA таблицы в PROD (лишние)
    FOR r IN (
        SELECT table_name FROM all_tables WHERE owner = UPPER(p_prod_schema)
        MINUS
        SELECT table_name FROM all_tables WHERE owner = UPPER(p_dev_schema)
    ) LOOP
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'TABLE';
        v_results(v_result_idx).obj_name := r.table_name;
        v_results(v_result_idx).status := 'EXTRA_IN_PROD';
        v_results(v_result_idx).details := 'Table exists in PROD but not in DEV';
        v_results(v_result_idx).ddl_script := generate_drop_ddl('TABLE', r.table_name);
        v_results(v_result_idx).sort_order := 100000; -- В самом конце
    END LOOP;

    -- ========================================================================
    -- ЗАДАНИЕ 2: СРАВНЕНИЕ ИНДЕКСОВ
    -- ========================================================================
    
    -- [7] MISSING индексы (исключаем системные SYS_%)
    FOR r IN (
        SELECT index_name FROM all_indexes 
        WHERE owner = UPPER(p_dev_schema) 
          AND index_name NOT LIKE 'SYS_%'
        MINUS
        SELECT index_name FROM all_indexes 
        WHERE owner = UPPER(p_prod_schema)
          AND index_name NOT LIKE 'SYS_%'
    ) LOOP
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'INDEX';
        v_results(v_result_idx).obj_name := r.index_name;
        v_results(v_result_idx).status := 'MISSING';
        v_results(v_result_idx).details := 'Index missing in PROD schema';
        v_results(v_result_idx).ddl_script := generate_create_index_ddl(p_dev_schema, r.index_name);
        v_results(v_result_idx).sort_order := 10000 + v_result_idx; -- После таблиц
    END LOOP;

    -- [8] EXTRA индексы в PROD
    FOR r IN (
        SELECT index_name FROM all_indexes 
        WHERE owner = UPPER(p_prod_schema)
          AND index_name NOT LIKE 'SYS_%'
        MINUS
        SELECT index_name FROM all_indexes 
        WHERE owner = UPPER(p_dev_schema)
          AND index_name NOT LIKE 'SYS_%'
    ) LOOP
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'INDEX';
        v_results(v_result_idx).obj_name := r.index_name;
        v_results(v_result_idx).status := 'EXTRA_IN_PROD';
        v_results(v_result_idx).details := 'Index exists in PROD but not in DEV';
        v_results(v_result_idx).ddl_script := generate_drop_ddl('INDEX', r.index_name);
        v_results(v_result_idx).sort_order := 110000;
    END LOOP;

    -- ========================================================================
    -- ЗАДАНИЕ 2: СРАВНЕНИЕ ПАКЕТОВ
    -- ========================================================================
    
    -- [9] MISSING пакеты
    FOR r IN (
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_dev_schema) AND object_type = 'PACKAGE'
        MINUS
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_prod_schema) AND object_type = 'PACKAGE'
    ) LOOP
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'PACKAGE';
        v_results(v_result_idx).obj_name := r.object_name;
        v_results(v_result_idx).status := 'MISSING';
        v_results(v_result_idx).details := 'Package missing in PROD schema';
        v_results(v_result_idx).ddl_script := get_ddl_via_metadata('PACKAGE', r.object_name, p_dev_schema);
        v_results(v_result_idx).sort_order := 20000 + v_result_idx;
    END LOOP;

    -- [10] EXTRA пакеты в PROD
    FOR r IN (
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_prod_schema) AND object_type = 'PACKAGE'
        MINUS
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_dev_schema) AND object_type = 'PACKAGE'
    ) LOOP
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'PACKAGE';
        v_results(v_result_idx).obj_name := r.object_name;
        v_results(v_result_idx).status := 'EXTRA_IN_PROD';
        v_results(v_result_idx).details := 'Package exists in PROD but not in DEV';
        v_results(v_result_idx).ddl_script := generate_drop_ddl('PACKAGE', r.object_name);
        v_results(v_result_idx).sort_order := 120000;
    END LOOP;

    -- ========================================================================
    -- ЗАДАНИЕ 2: СРАВНЕНИЕ ПРОЦЕДУР
    -- ========================================================================
    
    -- [11] MISSING процедуры (используем all_objects, т.к. all_procedures не содержит object_type)
    FOR r IN (
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_dev_schema) AND object_type = 'PROCEDURE'
        MINUS
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_prod_schema) AND object_type = 'PROCEDURE'
    ) LOOP
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'PROCEDURE';
        v_results(v_result_idx).obj_name := r.object_name;
        v_results(v_result_idx).status := 'MISSING';
        v_results(v_result_idx).details := 'Procedure missing in PROD schema';
        v_results(v_result_idx).ddl_script := get_ddl_via_metadata('PROCEDURE', r.object_name, p_dev_schema);
        v_results(v_result_idx).sort_order := 30000 + v_result_idx;
    END LOOP;

    -- [12] EXTRA процедуры в PROD
    FOR r IN (
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_prod_schema) AND object_type = 'PROCEDURE'
        MINUS
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_dev_schema) AND object_type = 'PROCEDURE'
    ) LOOP
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'PROCEDURE';
        v_results(v_result_idx).obj_name := r.object_name;
        v_results(v_result_idx).status := 'EXTRA_IN_PROD';
        v_results(v_result_idx).details := 'Procedure exists in PROD but not in DEV';
        v_results(v_result_idx).ddl_script := generate_drop_ddl('PROCEDURE', r.object_name);
        v_results(v_result_idx).sort_order := 130000;
    END LOOP;

    -- ========================================================================
    -- ЗАДАНИЕ 2: СРАВНЕНИЕ ФУНКЦИЙ
    -- ========================================================================
    
    -- [13] MISSING функции (используем all_objects, т.к. all_procedures не содержит object_type)
    FOR r IN (
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_dev_schema) AND object_type = 'FUNCTION'
        MINUS
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_prod_schema) AND object_type = 'FUNCTION'
    ) LOOP
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'FUNCTION';
        v_results(v_result_idx).obj_name := r.object_name;
        v_results(v_result_idx).status := 'MISSING';
        v_results(v_result_idx).details := 'Function missing in PROD schema';
        v_results(v_result_idx).ddl_script := get_ddl_via_metadata('FUNCTION', r.object_name, p_dev_schema);
        v_results(v_result_idx).sort_order := 40000 + v_result_idx;
    END LOOP;

    -- [14] EXTRA функции в PROD
    FOR r IN (
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_prod_schema) AND object_type = 'FUNCTION'
        MINUS
        SELECT object_name FROM all_objects 
        WHERE owner = UPPER(p_dev_schema) AND object_type = 'FUNCTION'
    ) LOOP
        v_result_idx := v_result_idx + 1;
        v_results(v_result_idx).obj_type := 'FUNCTION';
        v_results(v_result_idx).obj_name := r.object_name;
        v_results(v_result_idx).status := 'EXTRA_IN_PROD';
        v_results(v_result_idx).details := 'Function exists in PROD but not in DEV';
        v_results(v_result_idx).ddl_script := generate_drop_ddl('FUNCTION', r.object_name);
        v_results(v_result_idx).sort_order := 140000;
    END LOOP;

    -- ========================================================================
    -- ЗАДАНИЕ 3: ВЫВОД РЕЗУЛЬТАТОВ
    -- ========================================================================
    
    -- Выводим все собранные результаты
    FOR i IN 1..v_result_idx LOOP
        PIPE ROW(t_schema_comp_row(
            v_results(i).obj_type,
            v_results(i).obj_name,
            v_results(i).status,
            v_results(i).details,
            v_results(i).ddl_script,
            v_results(i).sort_order
        ));
    END LOOP;

    RETURN;
END compare_schemas_oracle;
/

-- ============================================================================
-- ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ
-- ============================================================================
-- 
-- 1. Выполните весь скрипт для создания типов и функции:
--    @schema_compare.sql
--
-- 2. Запустите сравнение схем:
   SELECT * FROM TABLE(compare_schemas_oracle('DEV_USER', 'PROD_USER'))
   ORDER BY sort_order;
--
-- 3. для экспорта ddl-скриптов в файл:
--    spool sync_prod.sql
--    select ddl_script from table(compare_schemas_oracle('dev_user', 'prod_user'))
--    WHERE ddl_script IS NOT NULL
--    ORDER BY sort_order;
--    SPOOL OFF
--
-- 4. Интерпретация результатов:
--    - MISSING: Объект отсутствует в PROD, нужно создать (CREATE)
--    - DIFFERENT: Объект отличается структурой, нужно изменить (ALTER)
--    - EXTRA_IN_PROD: Объект есть в PROD, но отсутствует в DEV (DROP)
--
-- 5. Порядок выполнения (sort_order):
--    - 1-9999: Таблицы (топологически отсортированы, независимые первыми)
--    - 10000-19999: Индексы
--    - 20000-29999: Пакеты
--    - 30000-39999: Процедуры
--    - 40000-49999: Функции
--    - 100000+: Объекты для удаления (EXTRA_IN_PROD)
--
-- ПРИМЕЧАНИЯ:
-- - Для процедур/функций/пакетов используется DBMS_METADATA.GET_DDL
-- - Требуется привилегия SELECT на ALL_* словари для обеих схем
-- - Требуется привилегия EXECUTE на DBMS_METADATA
-- - При наличии циклических зависимостей FK будет выведена ошибка
-- ============================================================================