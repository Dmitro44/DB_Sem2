CREATE OR REPLACE FUNCTION check_odd_even
RETURN VARCHAR2
IS
    v_even_count NUMBER;
    v_odd_count NUMBER;
BEGIN
    SELECT
        SUM(CASE WHEN MOD(val, 2) = 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN MOD(val, 2) != 0 THEN 1 ELSE 0 END)
    INTO v_even_count, v_odd_count
    FROM MYTABLE;

    IF v_even_count > v_odd_count THEN
        RETURN 'TRUE';
    ELSIF v_odd_count > v_even_count THEN
        RETURN 'FALSE';
    ELSE
        RETURN 'EQUAL';
    END IF;
end;

SELECT check_odd_even();
