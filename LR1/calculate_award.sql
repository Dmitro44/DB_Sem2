CREATE OR REPLACE FUNCTION calc_award(month_salary NUMBER, bonus_perc NUMBER)
RETURN NUMBER
IS
    v_bonus_float NUMBER := bonus_perc / 100;
    v_result NUMBER;
BEGIN
    if bonus_perc != TRUNC(bonus_perc) THEN
        RAISE_APPLICATION_ERROR(-20003, 'Error: Bonus percentage (' || bonus_perc || ') should be integer');
    end if;

    if bonus_perc < 0 OR bonus_perc > 100 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Error: Bonus percentage (' || bonus_perc || ') should be in range from 0 to 100');
    end if;

    if month_salary IS NULL OR month_salary <= 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Error: Monthly salary (' || month_salary || ') should be positive number');
    end if;

    v_result := (1 + v_bonus_float) * 12 * month_salary;

    RETURN v_result;
end calc_award;
/


DECLARE
    v_result NUMBER;
BEGIN
    BEGIN
        v_result := calc_award(5000, 10.5);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Intercepted error: ' || SQLERRM);
    end;

    BEGIN
        v_result := calc_award(5000, 130);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Intercepted error: ' || SQLERRM);
    end;

    v_result := calc_award(5000, 10);
    DBMS_OUTPUT.PUT_LINE('Result: ' || v_result);
end;