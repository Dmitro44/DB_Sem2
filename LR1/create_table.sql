BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE MyTable (
            id NUMBER PRIMARY KEY,
            val NUMBER
        )';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Table already exists');
        ELSE
            RAISE;
        end if;
end;

CREATE TABLE MyTable (
    id NUMBER PRIMARY KEY,
    val NUMBER
);

DROP TABLE MYTABLE PURGE;

PURGE RECYCLEBIN;

