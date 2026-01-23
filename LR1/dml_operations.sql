CREATE OR REPLACE PROCEDURE insert_proc(p_id IN MYTABLE.ID%type, p_val IN MYTABLE.VAL%type)
IS
BEGIN
    INSERT INTO MYTABLE (id, val)
    VALUES (p_id, p_val);

    COMMIT;
EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Line with ID ' || p_id || ' already exists');
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
end insert_proc;
/

BEGIN
    insert_proc(10002, 843);
end;

CREATE OR REPLACE PROCEDURE update_proc(p_id IN MYTABLE.ID%type, p_val IN MYTABLE.VAL%type)
IS
BEGIN
    UPDATE MYTABLE
    SET val = p_val
    WHERE ID = p_id;

    IF SQL%ROWCOUNT = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Update was not completed. ID ' || p_id || ' not found');
    ELSE
        COMMIT;
    end if;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
end update_proc;
/

BEGIN
    update_proc(10001, 2);
end;

CREATE OR REPLACE PROCEDURE delete_proc(p_id IN MYTABLE.ID%type)
IS
BEGIN
    DELETE FROM MYTABLE
    WHERE ID = p_id;

    IF SQL%ROWCOUNT = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Delete was not completed. ID ' || p_id || ' not found');
    ELSE
        COMMIT;
    end if;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
end delete_proc;
/

BEGIN
    delete_proc(10001);
end;
