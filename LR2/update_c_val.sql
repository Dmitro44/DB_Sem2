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
/
