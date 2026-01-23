DECLARE
    TYPE t_ids IS TABLE OF NUMBER;
    TYPE t_vals IS TABLE OF NUMBER;

    v_ids t_ids := t_ids();
    v_vals t_vals := t_vals();
BEGIN
    FOR i IN 1..10000 LOOP
        v_ids.extend;
        v_vals.extend;

        v_ids(i) := i;
        v_vals(i) := ROUND(DBMS_RANDOM.VALUE(0, 500000));
    end loop;

    FORALL i IN 1..v_ids.COUNT
        INSERT INTO MYTABLE (id, val)
        VALUES (v_ids(i), v_vals(i));

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Inserted ' || SQL%ROWCOUNT || 'entries');
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error ' || SQLERRM);
end;


-- DELETE
BEGIN
    DELETE FROM MyTable
    WHERE ROWNUM <= 10000;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Удалено ' || SQL%ROWCOUNT || ' строк');
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
END;

TRUNCATE TABLE MYTABLE;
