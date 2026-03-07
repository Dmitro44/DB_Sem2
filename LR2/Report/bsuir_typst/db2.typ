#import "stp/stp2024.typ"
#show: stp2024.template

#include("lab_title.typ")

#pagebreak()

= Индивидуальное задание

+ #[Создать таблицы #emph("STUDENTS") и #emph("GROUPS") с соответствующими полями.]
+ #[Реализовать триггеры для обеспечения целостности данных: автоматическая генерация #emph("ID"), проверка уникальности #emph("ID") и имен групп.]
+ #[Реализовать #emph("ForeignKey") с каскадным удалением студентов при удалении группы.]
+ #[Реализовать триггер, реализующий журналирование всех действий над данными таблицы #emph("STUDENTS").]
+ #[Исходя из данных предыдущей задачи, реализовать процедуру для восстановления информации на указанный временной момент и на временное смещение.]
+ #[Реализовать триггер, который в случае изменения данных в таблице #emph("STUDENTS") будет соответственно обновлять информацию #emph("C_VAL") таблицы #emph("GROUPS").]

= Краткие теоретические сведения

Триггеры в #emph("Oracle") -- это специальные хранимые процедуры, которые автоматически запускаются при возникновении определенных событий (#emph("INSERT"), #emph("UPDATE"), #emph("DELETE")). Они классифицируются по нескольким признакам:
- #[по времени срабатывания: #emph("BEFORE") (выполняются до операции, подходят для валидации данных и изменения значений #emph(":NEW")) и #emph("AFTER") (выполняются после операции, используются для аудита и каскадных обновлений);]
- #[по уровню влияния: #emph("FOR EACH ROW") (строчные -- выполняются для каждой измененной строки) и операторные (выполняются один раз на весь SQL-запрос).]

При разработке триггеров часто возникает ошибка #emph("ORA-04091") (#emph("mutating table")). Данная ситуация наступает, когда строчный триггер пытается прочитать или изменить таблицу, которая в данный момент уже находится в процессе изменения этим же или связанным запросом. Это защитный механизм СУБД, предотвращающий чтение логически несогласованных данных. Для обхода этой проблемы используются автономные транзакции или уточнение условий срабатывания триггера (например, #emph("UPDATE OF column_name")).

Аудит данных позволяет реализовать механизм восстановления на момент времени. Сохраняя старое (#emph(":OLD")) и новое (#emph(":NEW")) состояние строк, формируется цепочка изменений. Восстановление в этом случае заключается в проигрывании истории в обратном порядке: для отмены #emph("DELETE") выполняется #emph("INSERT"), для отмены #emph("INSERT") -- #emph("DELETE"), а для отмены #emph("UPDATE") -- возврат исходных значений полей. Важным условием при ручном восстановлении является временное отключение ограничений целостности и триггеров для предотвращения зацикливания операций аудита.

= Выполнение работы

== Создание таблиц и триггеров целостности

Были созданы таблицы #emph("STUDENTS") и #emph("GROUPS"). Для каждой таблицы реализованы триггеры для проверки уникальности #emph("ID") и автоматической генерации значений через последовательности.

#stp2024.listing[Триггер проверки уникальности ID студентов][
  ```
  CREATE OR REPLACE TRIGGER check_integrity_students
  BEFORE INSERT OR UPDATE ON STUDENTS
  FOR EACH ROW
  DECLARE
      v_count NUMBER;
  BEGIN
      SELECT COUNT(*)
      INTO v_count
      FROM STUDENTS
      WHERE id = :NEW.id
      AND (:OLD.id IS NULL OR id != :OLD.id);

      IF v_count > 0 THEN
          RAISE_APPLICATION_ERROR(-20004, 'Error: ID ' || :NEW.id || ' already exists in STUDENTS table');
      end if;
  end;
  ```
]

#stp2024.listing[Триггер проверки уникальности имен групп][
  ```
  CREATE OR REPLACE TRIGGER check_group_name
      BEFORE INSERT OR UPDATE OF name ON GROUPS
      FOR EACH ROW
  DECLARE
      v_count NUMBER;
  BEGIN
      SELECT COUNT(*) INTO v_count FROM GROUPS
      WHERE name = :NEW.name AND (:OLD.id IS NULL OR id != :OLD.id);

      IF v_count > 0 THEN
          RAISE_APPLICATION_ERROR(-20005, 'Error: Group name "' || :NEW.name || '" already exists');
      END IF;
  END;
  ```
]

== Реализация аудита изменений

Для ведения логов были созданы таблицы #emph("students_audit") и #emph("groups_audit"), а также триггеры #emph("AFTER"), которые срабатывают после выполнения любой DML-операции.

#stp2024.listing[Триггер аудита для таблицы GROUPS][
  ```
CREATE OR REPLACE TRIGGER trg_groups_audit
    AFTER INSERT OR UPDATE OR DELETE ON GROUPS
    FOR EACH ROW
DECLARE
    v_opt_type VARCHAR2(10);
BEGIN
    IF INSERTING THEN
        v_opt_type := 'INSERT';
    ELSIF UPDATING THEN
        v_opt_type := 'UPDATE';
    ELSIF DELETING THEN
        v_opt_type := 'DELETE';
    end if;

    INSERT INTO GROUPS_AUDIT(
            id,
            operation_type,
            operation_timestamp,
            db_user,
            old_id, old_name, old_c_val,
            new_id, new_name, new_c_val)
    VALUES (
            groups_audit_seq.nextval,
            v_opt_type,
            SYSTIMESTAMP,
            USER,
            :OLD.id, :OLD.name, :OLD.c_val,
            :NEW.id, :NEW.name, :NEW.c_val);
end;
  ```
]

== Процедура восстановления состояния

Реализована процедура #emph("restore_full_state"), которая принимает временную метку или интервал. Основная особенность -- восстановление групп перед студентами и отключение триггеров на время выполнения для предотвращения «зацикливания» аудита и ошибок мутации таблиц.

#stp2024.listing[Процедура восстановления групп и студентов][
  ```
CREATE OR REPLACE PROCEDURE restore_full_state(
    p_restore_timestamp IN TIMESTAMP,
    p_interval_offset IN INTERVAL DAY TO SECOND
)
IS
    v_target_timestamp TIMESTAMP;
BEGIN
    IF p_restore_timestamp IS NOT NULL AND p_interval_offset IS NOT NULL THEN
        RAISE_APPLICATION_ERROR(-20006, 'Error: Specify either exact time or offset, but not both');
    ELSIF p_restore_timestamp IS NOT NULL THEN
        v_target_timestamp := p_restore_timestamp;
    ELSIF p_interval_offset IS NOT NULL THEN
        v_target_timestamp := SYSTIMESTAMP - p_interval_offset;
    ELSE
        RAISE_APPLICATION_ERROR(-20007, 'Error: Need to specify either the exact time or offset');
    end if;

    DBMS_OUTPUT.PUT_LINE('Restoring GROUPS table state to ' || TO_CHAR(v_target_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF'));

    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE GROUPS DISABLE ALL TRIGGERS';
        EXECUTE IMMEDIATE 'ALTER TABLE STUDENTS DISABLE ALL TRIGGERS';
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Warning: unable to disable triggers on GROUPS and STUDENTS.');
    end;

    FOR rec IN (SELECT * FROM groups_audit
                WHERE operation_timestamp > v_target_timestamp
                ORDER BY operation_timestamp DESC, id DESC)
        LOOP
        IF rec.operation_type = 'INSERT' THEN
            DELETE FROM GROUPS WHERE id = rec.new_id;
            DBMS_OUTPUT.PUT_LINE('Restore INSERT: Deleted group with ID ' || rec.new_id);
        ELSIF rec.operation_type = 'DELETE' THEN
            INSERT INTO GROUPS (id, name, c_val)
            VALUES (rec.old_id, rec.old_name, rec.old_c_val);
            DBMS_OUTPUT.PUT_LINE('Restore DELETE: Inserted group with ID ' || rec.old_id);
        ELSIF rec.operation_type = 'UPDATE' THEN
            UPDATE GROUPS
            SET name = rec.old_name,
                c_val = rec.old_c_val
            WHERE id = rec.old_id;
            DBMS_OUTPUT.PUT_LINE('Restore UPDATE: Restored group with ID ' || rec.old_id);
        end if;
    end loop;

    FOR rec IN (SELECT * FROM students_audit
                WHERE operation_timestamp > v_target_timestamp
                ORDER BY operation_timestamp DESC, id DESC)
        LOOP
        if rec.operation_type = 'INSERT' THEN
            DELETE FROM STUDENTS WHERE id = rec.new_id;
            DBMS_OUTPUT.PUT_LINE('Restore INSERT: Deleted student with ID ' || rec.new_id);
        ELSIF rec.operation_type = 'DELETE' THEN
            INSERT INTO STUDENTS (id, group_id, name)
            VALUES (rec.old_id, rec.old_group_id, rec.old_name);
            DBMS_OUTPUT.PUT_LINE('Restore DELETE: Inserted student with ID ' || rec.old_id);
        ELSIF rec.operation_type = 'UPDATE' THEN
            UPDATE STUDENTS
            SET GROUP_ID = rec.old_group_id,
                name = rec.old_name
            WHERE id = rec.old_id;
            DBMS_OUTPUT.PUT_LINE('Restore UPDATE: Restored student with ID ' || rec.old_id);
        end if;
    end loop;

    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE GROUPS ENABLE ALL TRIGGERS';
        EXECUTE IMMEDIATE 'ALTER TABLE STUDENTS ENABLE ALL TRIGGERS';
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Warning: unable to enable triggers on GROUPS and STUDENTS.');
    end;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        EXECUTE IMMEDIATE 'ALTER TABLE GROUPS ENABLE ALL TRIGGERS';
        EXECUTE IMMEDIATE 'ALTER TABLE STUDENTS ENABLE ALL TRIGGERS';
        RAISE;
end;
  ```
]

== Автоматическое обновление счетчика C_VAL

Для поддержания актуального количества студентов в группах реализован триггер на таблицу #emph("STUDENTS"). Он учитывает вставку, удаление и перемещение студента между группами.

#stp2024.listing[Триггер обновления C_VAL в таблице GROUPS][
  ```
  CREATE OR REPLACE TRIGGER trg_update_group_c_val
  AFTER INSERT OR UPDATE OR DELETE ON STUDENTS
  FOR EACH ROW
  BEGIN
      IF INSERTING THEN
          UPDATE GROUPS 
          SET c_val = NVL(c_val, 0) + 1 
          WHERE id = :NEW.group_id;
      ELSIF DELETING THEN
          UPDATE GROUPS 
          SET c_val = GREATEST(NVL(c_val, 0) - 1, 0)
          WHERE id = :OLD.group_id;
      ELSIF UPDATING THEN
          IF :OLD.group_id != :NEW.group_id THEN
              UPDATE GROUPS
              SET c_val = GREATEST(NVL(c_val, 0) - 1, 0)
              WHERE id = :OLD.group_id;
              
              UPDATE GROUPS
              SET c_val = NVL(c_val, 0) + 1 
              WHERE id = :NEW.group_id;
          END IF;
      END IF;
  EXCEPTION
      WHEN OTHERS THEN
          IF SQLCODE = -4091 THEN
              NULL; -- Пропускаем ошибку мутации при каскадном удалении
          ELSE
              RAISE;
          END IF;
  END;
  ```
]

#stp2024.heading_unnumbered[Вывод]

В ходе выполнения лабораторной работы были изучены и применены на практике механизмы создания триггеров в СУБД #emph("Oracle"). Были реализованы строчные триггеры типов #emph("BEFORE") и #emph("AFTER") для автоматизации процессов обеспечения целостности данных и ведения подробного журнала аудита.

Особое внимание было уделено обработке сложных ситуаций, таких как каскадное удаление записей и перемещение студентов между группами, что потребовало использования функций #emph("NVL") и #emph("GREATEST") для предотвращения логических ошибок в счетчиках.

Была разработана процедура восстановления данных, позволяющая возвращать состояние таблиц на любой момент времени в прошлом. В процессе реализации было выявлено, что критически важным является соблюдение иерархии объектов (сначала восстановление групп, затем связанных с ними студентов) и временное отключение триггеров с помощью #emph("EXECUTE IMMEDIATE"). Это позволило избежать рекурсивных вызовов аудита и конфликтов с бизнес-логикой системы. Изучение ошибки мутации таблиц #emph("ORA-04091") позволило глубже понять архитектуру работы транзакций в #emph("Oracle") и освоить методы обхода данного ограничения.

