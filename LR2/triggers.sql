CREATE OR REPLACE TRIGGER check_integrity_students
BEFORE INSERT OR UPDATE ON STUDENTS
FOR EACH ROW
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM STUDENTS
    WHERE id = :NEW.id;

    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20004, 'Error: ID ' || :NEW.id || ' already exists in STUDENTS table');
    end if;
end;
/

BEGIN
    INSERT INTO STUDENTS (GROUP_ID, NAME)
    VALUES (1, 'Lesha');
    COMMIT;
end;

BEGIN
    INSERT INTO STUDENTS (ID, GROUP_ID, NAME)
    VALUES (1, 1, 'Dmitry');
    COMMIT;
end;

CREATE SEQUENCE students_seq START WITH 1 INCREMENT BY 1;
ALTER SEQUENCE students_seq RESTART;

CREATE OR REPLACE TRIGGER students_autoinc
BEFORE INSERT ON STUDENTS
FOR EACH ROW
BEGIN
    if :NEW.ID IS NULL THEN
        :NEW.ID := students_seq.nextval;
    end if;
end;


-- Groups table

CREATE OR REPLACE TRIGGER check_integrity_groups
    BEFORE INSERT OR UPDATE ON GROUPS
    FOR EACH ROW
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM GROUPS
    WHERE id = :NEW.id;

    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20004, 'Error: ID ' || :NEW.id || ' already exists in GROUPS table');
    end if;
end;

-- Insert in groups
BEGIN
    INSERT INTO GROUPS (C_VAL, NAME)
    VALUES (30, '353503');
    COMMIT;
end;

BEGIN
    DELETE FROM GROUPS WHERE id = 1;
    COMMIT;
end;

BEGIN
    INSERT INTO GROUPS (ID, C_VAL, NAME)
    VALUES (1, 30, '353503');
    COMMIT;
end;


CREATE SEQUENCE groups_seq START WITH 1 INCREMENT BY 1;
ALTER SEQUENCE groups_seq RESTART;

CREATE OR REPLACE TRIGGER groups_autoinc
    BEFORE INSERT ON GROUPS
    FOR EACH ROW
BEGIN
    if :NEW.id IS NULL THEN
        :NEW.id := groups_seq.nextval;
    end if;
end;

CREATE OR REPLACE TRIGGER check_group_name
    BEFORE INSERT OR UPDATE ON GROUPS
    FOR EACH ROW
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM GROUPS
    WHERE name = :NEW.name
    AND (:OLD.id IS NULL OR ID != :OLD.id);

    if v_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20005, 'Error: Group name "' || :NEW.name || '" already exists in GROUPS table');
    end if;
end;


-- DROP TRIGGER check_integrity_students;
-- DROP TRIGGER students_autoinc;
-- DROP TRIGGER check_integrity_groups;
-- DROP TRIGGER groups_autoinc;
-- DROP TRIGGER check_group_name;
