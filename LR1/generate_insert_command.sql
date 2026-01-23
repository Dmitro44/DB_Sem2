CREATE OR REPLACE PROCEDURE gen_insert_cmd(p_id IN MYTABLE.ID%type)
IS
    v_val MYTABLE.VAL%type;
    v_insert_command VARCHAR2(500);
BEGIN
    SELECT VAL
    INTO v_val
    FROM MYTABLE
    WHERE id = p_id;

    v_insert_command := 'INSERT INTO MYTABLE (id, val) VALUES (' || p_id || ', ' || v_val || ');';

    DBMS_OUTPUT.PUT_LINE(v_insert_command);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Line with ID ' || p_id || ' not found');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
end;
/

BEGIN
    gen_insert_cmd(205);
end;
