#import "lib/stp2024.typ"
#show: stp2024.template

#include "lab_title.typ"

#pagebreak()

= Индивидуальное задание

+ #[Написать функцию, на вход которой подаются два текстовых параметра (#emph("dev_schema_name"), #emph("prod_schema_name")), являющихся названиями схем баз данных. На выход предоставить перечень таблиц, которые есть в схеме Dev, но нет в Prod, либо в которых различается структура таблиц. Наименования таблиц должны быть отсортированы в соответствии с очередностью их возможного создания в схеме Prod (необходимо учитывать #emph("FOREIGN KEY") в схеме). В случае закольцованных связей выводить соответствующее сообщение.]
+ #[Доработать предыдущий скрипт с учетом возможности сравнения не только таблиц, но и процедур, функций, индексов, пакетов.]
+ #[Доработать предыдущий скрипт с генерацией #emph("DDL")-скрипта на обновление объектов, а также с учетом необходимости удаления в схеме Prod объектов, отсутствующих в схеме Dev.]

= Краткие теоретические сведения

== Схемы данных в Oracle

В СУБД #emph("Oracle") схема представляет собой логическую совокупность объектов базы данных (таблиц, представлений, индексов, процедур и т.д.), принадлежащих конкретному пользователю. Каждый пользователь имеет собственную схему, имя которой совпадает с именем пользователя. Для обращения к объектам чужой схемы используется синтаксис #emph("schema_name.object_name").

== Словарные представления Oracle

Для получения метаданных об объектах базы данных используются системные представления (#emph("data dictionary views")):

- #emph("ALL_TABLES") — содержит информацию о таблицах, доступных текущему пользователю;
- #emph("ALL_TAB_COLUMNS") — описывает колонки всех таблиц с указанием типа данных, длины и признака #emph("NULL");
- #emph("ALL_CONSTRAINTS") — содержит информацию об ограничениях целостности, включая внешние ключи (#emph("constraint_type = 'R'"));
- #emph("ALL_INDEXES") — описывает индексы, созданные в базе данных;
- #emph("ALL_OBJECTS") — содержит информацию обо всех объектах базы данных.

== Топологическая сортировка и обход в глубину

При определении порядка создания таблиц необходимо учитывать зависимости через внешние ключи (#emph("FOREIGN KEY")). Таблица, на которую ссылаются, должна быть создана ранее ссылающейся таблицы.

Для определения порядка используется алгоритм топологической сортировки на основе обхода в глубину (#emph("Depth-First Search")). Вершины графа представляют таблицы, а рёбра — зависимости через внешние ключи. Алгоритм использует три состояния вершины:

- #emph("Белая") (0) — вершина не посещена;
- #emph("Серая") (1) — вершина в текущем стеке рекурсии;
- #emph("Чёрная") (2) — вершина полностью обработана.

Попадание в серую вершину при обходе означает наличие циклической зависимости.

== Pipelined-функции в Oracle

Pipelined-функции позволяют возвращать результаты по мере их вычисления, не дожидаясь полного завершения функции. Это обеспечивает эффективную обработку больших объёмов данных и позволяет использовать функции в операторах #emph("SELECT") через синтаксис #emph("TABLE()").

= Выполнение работы

== Создание типов данных

Для возврата структурированного результата из функции были созданы пользовательские типы: #emph("t_schema_comp_row") для одной строки результата и #emph("t_schema_comp_table") для коллекции таких строк.

#stp2024.listing[Определение типов для возврата результата][
  ```
  CREATE OR REPLACE TYPE t_schema_comp_row AS OBJECT (
      table_name  VARCHAR2(128),
      status      VARCHAR2(50),
      details     CLOB,
      sort_order  NUMBER
  );
  /

  CREATE OR REPLACE TYPE t_schema_comp_table AS TABLE OF t_schema_comp_row;
  /
  ```
]

== Создание схем DEV и PROD

Для тестирования функции сравнения были созданы две схемы: #emph("DEV_USER") (разработка) и #emph("PROD_USER") (промышленная). Схемы содержат идентичные таблицы #emph("DEPARTMENTS"), #emph("STUDENTS"), #emph("COURSES") и #emph("ENROLLMENTS") с внешними ключами.

Для имитации различий в схеме #emph("DEV_USER") были добавлены:
- колонка #emph("EMAIL") в таблицу #emph("STUDENTS");
- колонка #emph("GRADE") в таблицу #emph("ENROLLMENTS");
- таблица #emph("INSTRUCTORS"), отсутствующая в #emph("PROD_USER").

#stp2024.listing[Создание таблиц в схеме DEV_USER][
  ```
  CREATE TABLE DEPARTMENTS (
      DEPT_ID   NUMBER(10) PRIMARY KEY,
      DEPT_NAME VARCHAR2(100) NOT NULL,
      BUILDING  VARCHAR2(50)
  );

  CREATE TABLE STUDENTS (
      STUDENT_ID   NUMBER(10) PRIMARY KEY,
      FIRST_NAME   VARCHAR2(50) NOT NULL,
      LAST_NAME    VARCHAR2(50) NOT NULL,
      EMAIL        VARCHAR2(100),  -- отсутствует в PROD
      DEPT_ID      NUMBER(10),
      CONSTRAINT FK_STUDENTS_DEPT FOREIGN KEY (DEPT_ID) 
          REFERENCES DEPARTMENTS(DEPT_ID)
  );

  CREATE TABLE COURSES (
      COURSE_ID   NUMBER(10) PRIMARY KEY,
      COURSE_NAME VARCHAR2(100) NOT NULL,
      CREDITS     NUMBER(2),
      DEPT_ID     NUMBER(10),
      CONSTRAINT FK_COURSES_DEPT FOREIGN KEY (DEPT_ID) 
          REFERENCES DEPARTMENTS(DEPT_ID)
  );

  CREATE TABLE INSTRUCTORS (
      INSTRUCTOR_ID NUMBER(10) PRIMARY KEY,
      FIRST_NAME    VARCHAR2(50) NOT NULL,
      LAST_NAME     VARCHAR2(50) NOT NULL,
      HIRE_DATE     DATE,
      DEPT_ID       NUMBER(10),
      CONSTRAINT FK_INSTRUCTORS_DEPT FOREIGN KEY (DEPT_ID) 
          REFERENCES DEPARTMENTS(DEPT_ID)
  );

  CREATE TABLE ENROLLMENTS (
      ENROLLMENT_ID   NUMBER(10) PRIMARY KEY,
      STUDENT_ID      NUMBER(10) NOT NULL,
      COURSE_ID       NUMBER(10) NOT NULL,
      ENROLLMENT_DATE DATE,
      GRADE           VARCHAR2(2),  -- отсутствует в PROD
      CONSTRAINT FK_ENROLL_STUDENT FOREIGN KEY (STUDENT_ID) 
          REFERENCES STUDENTS(STUDENT_ID),
      CONSTRAINT FK_ENROLL_COURSE FOREIGN KEY (COURSE_ID) 
          REFERENCES COURSES(COURSE_ID)
  );
  ```
]

== Определение структурных различий

Для поиска различий между схемами используются операции #emph("MINUS") и #emph("LEFT JOIN") с системными представлениями. Функция определяет три типа различий:

#figure(
  table(
    columns: (auto, auto, auto),
    table.header([*Тип*], [*Описание*], [*Пример*]),
    [MISSING], [Объект есть в DEV, отсутствует в PROD], [Таблица INSTRUCTORS],
    [DIFFERENT], [Структура объекта различается], [Добавлена колонка EMAIL],
    [EXTRA_IN_PROD], [Объект есть в PROD, отсутствует в DEV], [Устаревшая таблица],
  ),
  caption: [Типы различий между схемами]
) <fig:diff_types>

Определение таблиц, отсутствующих в Prod, выполняется с помощью операции #emph("MINUS"):

#stp2024.listing[Поиск отсутствующих таблиц в PROD][
  ```
  FOR r IN (
      SELECT table_name FROM all_tables WHERE owner = UPPER(p_dev_schema)
      MINUS
      SELECT table_name FROM all_tables WHERE owner = UPPER(p_prod_schema)
  ) LOOP
      v_tables_to_report(r.table_name) := 'Table missing in PROD';
  END LOOP;
  ```
]

Определение различий в структуре колонок выполняется через #emph("LEFT JOIN") с проверкой типа данных, длины и признака #emph("NULL"):

#stp2024.listing[Сравнение структуры колонок][
  ```
  FOR r IN (
      SELECT dev.table_name, 
             'Structure mismatch: ' || LISTAGG(dev.column_name || ' (' || 
             dev.data_type || ')', ', ') WITHIN GROUP (ORDER BY dev.column_id) as diff_info
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
      IF NOT v_tables_to_report.EXISTS(r.table_name) THEN
          v_tables_to_report(r.table_name) := r.diff_info;
      END IF;
  END LOOP;
  ```
]

== Топологическая сортировка с помощью DFS

Для определения порядка создания таблиц реализован алгоритм обхода в глубину. Рекурсивная процедура #emph("visit_table") обходит зависимости через внешние ключи и определяет порядок обработки таблиц:

#stp2024.listing[Рекурсивная процедура обхода зависимостей][
  ```
  PROCEDURE visit_table(p_tn IN VARCHAR2, p_stack_path IN VARCHAR2 DEFAULT '') IS
      v_current_path VARCHAR2(4000);
  BEGIN
      IF v_has_cycle THEN RETURN; END IF;
      
      -- Обнаружение цикла (попали в серую вершину)
      IF v_all_dev_tables.EXISTS(p_tn) AND v_all_dev_tables(p_tn) = 1 THEN
          v_has_cycle := TRUE;
          v_cycle_msg := 'Circular dependency: ' || p_stack_path || ' -> ' || p_tn;
          RETURN;
      END IF;

      -- Вершина еще белая (не посещена)
      IF v_all_dev_tables.EXISTS(p_tn) AND v_all_dev_tables(p_tn) = 0 THEN
          v_all_dev_tables(p_tn) := 1;  -- Переводим в серую
          v_current_path := p_stack_path || 
              (CASE WHEN p_stack_path IS NULL THEN '' ELSE ' -> ' END) || p_tn;

          -- Обходим все таблицы, на которые ссылается данная
          FOR r IN (
              SELECT DISTINCT r.table_name as parent_table
              FROM all_constraints c
              JOIN all_constraints r ON c.r_constraint_name = r.constraint_name 
                  AND c.r_owner = r.owner
              WHERE c.owner = UPPER(p_dev_schema)
                AND c.table_name = UPPER(p_tn)
                AND c.constraint_type = 'R'
                AND r.table_name <> c.table_name
          ) LOOP
              visit_table(r.parent_table, v_current_path);
          END LOOP;

          v_all_dev_tables(p_tn) := 2;  -- Переводим в черную
          v_sort_counter := v_sort_counter + 1;
          v_sorted_indices(p_tn) := v_sort_counter;
      END IF;
  END visit_table;
  ```
]

== Результат выполнения

При вызове функции сравнения:

#stp2024.listing[Вызов функции сравнения схем][
  ```sql
  SELECT * FROM TABLE(compare_schemas_oracle('DEV_USER', 'PROD_USER'))
  ORDER BY sort_order;
  ```
]

Были обнаружены следующие различия:

#figure(
  table(
    columns: (auto, auto, auto, auto),
    table.header([*TABLE_NAME*], [*STATUS*], [*DETAILS*], [*SORT_ORDER*]),
    [STUDENTS], [DIFFERENT], [Structure mismatch: EMAIL (VARCHAR2)], [3],
    [ENROLLMENTS], [DIFFERENT], [Structure mismatch: GRADE (VARCHAR2)], [4],
    [INSTRUCTORS], [MISSING], [Table missing in PROD], [5],
  ),
  caption: [Результат сравнения схем]
) <fig:result>

Таблицы отсортированы с учётом зависимостей через #emph("FOREIGN KEY"): сначала #emph("STUDENTS") (зависит от #emph("DEPARTMENTS")), затем #emph("ENROLLMENTS") (зависит от #emph("STUDENTS") и #emph("COURSES")), затем #emph("INSTRUCTORS").

#pagebreak()

#stp2024.heading_unnumbered[Вывод]

В ходе выполнения лабораторной работы была разработана функция для сравнения двух схем Oracle. Были изучены и применены на практике:

- Системные представления Oracle (#emph("ALL_TABLES"), #emph("ALL_TAB_COLUMNS"), #emph("ALL_CONSTRAINTS")) для получения метаданных базы данных;
- Операции множеств (#emph("MINUS")) для определения отсутствующих объектов;
- Алгоритм топологической сортировки на основе обхода в глубину для определения порядка создания таблиц с учётом внешних ключей;
- Pipelined-функции для эффективного возврата структурированных данных.

Разработанная функция позволяет автоматически определять различия между схемами и рекомендовать порядок создания/обновления объектов, что существенно упрощает процесс синхронизации баз данных между окружениями разработки и промышленной эксплуатации. Обнаружение циклических зависимостей предотвращает некорректное развертывание изменений, а учёт объектов, отсутствующих в схеме разработки, позволяет идентифицировать устаревшие элементы структуры.
