CREATE TABLE students_audit(
    id NUMBER PRIMARY KEY,
    operation_type VARCHAR2(10),
    operation_timestamp TIMESTAMP,
    db_user VARCHAR2(30),
    old_id NUMBER,
    old_group_id NUMBER,
    old_name VARCHAR2(20),
    new_id NUMBER,
    new_group_id NUMBER,
    new_name VARCHAR2(20)
);

CREATE SEQUENCE students_audit_seq START WITH 1 INCREMENT BY 1;
ALTER SEQUENCE students_audit_seq RESTART;


CREATE TABLE groups_audit(
    id NUMBER PRIMARY KEY,
    operation_type VARCHAR2(10),
    operation_timestamp TIMESTAMP,
    db_user VARCHAR2(30),
    old_id NUMBER,
    old_name VARCHAR2(20),
    old_c_val NUMBER,
    new_id NUMBER,
    new_name VARCHAR2(20),
    new_c_val NUMBER
);

CREATE OR REPLACE TRIGGER trg_students_audit
    AFTER INSERT OR UPDATE OR DELETE ON STUDENTS
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

    INSERT INTO STUDENTS_AUDIT(
            id,
            operation_type,
            operation_timestamp,
            db_user,
            old_id, old_group_id, old_name,
            new_id, new_group_id, new_name)
    VALUES (
            students_audit_seq.nextval,
            v_opt_type,
            SYSTIMESTAMP,
            USER,
            :OLD.id, :OLD.group_id, :OLD.name,
            :NEW.id, :NEW.group_id, :NEW.name);
end;
/

DROP TRIGGER TRG_STUDENTS_AUDIT;


CREATE OR REPLACE PROCEDURE restore_student_state(
    p_restore_timestamp IN TIMESTAMP,
    p_interval_offset IN INTERVAL DAY TO SECOND
)
IS
    v_target_timestamp TIMESTAMP;

    CURSOR c_audit_log IS
        SELECT *
        FROM students_audit
        WHERE operation_timestamp > v_target_timestamp
        ORDER BY operation_timestamp DESC, id DESC;
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

    DBMS_OUTPUT.PUT_LINE('Restoring STUDENTS table state to ' || TO_CHAR(v_target_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF'));

    BEGIN
        EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_STUDENTS_AUDIT DISABLE';
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Warning: unable to disable trigger trg_students_audit.');
    end;

    FOR rec IN c_audit_log LOOP
        IF rec.operation_type = 'INSERT' THEN
            DELETE FROM STUDENTS WHERE id = rec.new_id;
            DBMS_OUTPUT.PUT_LINE('Restore INSERT: Deleted row with ID ' || rec.new_id);
        ELSIF rec.operation_type = 'DELETE' THEN
            INSERT INTO STUDENTS (id, group_id, name)
            VALUES (rec.old_id, rec.old_group_id, rec.old_name);
            DBMS_OUTPUT.PUT_LINE('Restore DELETE: Inserted row with ID ' || rec.old_id);
        ELSIF rec.operation_type = 'UPDATE' THEN
            UPDATE STUDENTS
            SET GROUP_ID = rec.old_group_id,
                name = rec.old_name
            WHERE id = rec.old_id;
            DBMS_OUTPUT.PUT_LINE('Restore UPDATE: Restored row with ID ' || rec.old_id);
        end if;
    end loop;

    BEGIN
        EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_STUDENTS_AUDIT ENABLE';
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Warning: unable to enable trigger trg_students_audit');
    end;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        BEGIN
            EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_STUDENTS_AUDIT ENABLE';
        EXCEPTION
            WHEN OTHERS THEN NUll;
        end;

        RAISE;
end;

BEGIN
    restore_student_state(
        p_restore_timestamp => NULL,
        p_interval_offset => INTERVAL '11' MINUTE
    );
end;