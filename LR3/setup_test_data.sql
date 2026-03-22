/*******************************************************************************
 * ORACLE TEST DATA SETUP SCRIPT
 * Purpose: Create test schemas for DEV_USER and PROD_USER with intentional
 *          differences for schema comparison testing
 * 
 * EXECUTION INSTRUCTIONS:
 * 1. Connect as DEV_USER and run all sections marked "-- EXECUTE AS DEV_USER"
 * 2. Connect as PROD_USER and run all sections marked "-- EXECUTE AS PROD_USER"
 * 3. Verify setup using queries at the end of this script
 *
 * SCHEMA DIFFERENCES (for testing):
 * - INSTRUCTORS table: EXISTS in DEV, MISSING in PROD (test: TABLE MISSING)
 * - STUDENTS.EMAIL column: EXISTS in DEV, MISSING in PROD (test: DIFFERENT)
 * - ENROLLMENTS.GRADE column: EXISTS in DEV, MISSING in PROD (test: DIFFERENT)
 * - GET_STUDENT_COUNT procedure: EXISTS in DEV, MISSING in PROD (test: MISSING)
 * - GET_DEPT_NAME function: EXISTS in DEV, MISSING in PROD (test: MISSING)
 * - IDX_STUDENTS_NAME index: EXISTS in DEV, MISSING in PROD (test: MISSING)
 *
 * DEPENDENCY GRAPH (topological order):
 * Level 1: DEPARTMENTS (no dependencies)
 * Level 2: STUDENTS (FK -> DEPARTMENTS)
 *          COURSES (FK -> DEPARTMENTS)
 *          INSTRUCTORS (FK -> DEPARTMENTS) [DEV only]
 * Level 3: ENROLLMENTS (FK -> STUDENTS, COURSES)
 ******************************************************************************/

-------------------------------------------------------------------------------
-- SECTION 1: DEV_USER SCHEMA (FULL VERSION)
-------------------------------------------------------------------------------
-- EXECUTE AS DEV_USER

-- Clean up existing objects (if re-running script)
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE ENROLLMENTS CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSTRUCTORS CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE STUDENTS CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE COURSES CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE DEPARTMENTS CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

-- Level 1: Base table (no FK dependencies)
CREATE TABLE DEPARTMENTS (
    DEPT_ID         NUMBER(10) PRIMARY KEY,
    DEPT_NAME       VARCHAR2(100) NOT NULL,
    BUILDING        VARCHAR2(50)
);

-- Level 2: Tables with FK to DEPARTMENTS
CREATE TABLE STUDENTS (
    STUDENT_ID      NUMBER(10) PRIMARY KEY,
    FIRST_NAME      VARCHAR2(50) NOT NULL,
    LAST_NAME       VARCHAR2(50) NOT NULL,
    EMAIL           VARCHAR2(100),  -- COLUMN EXISTS ONLY IN DEV
    DEPT_ID         NUMBER(10),
    CONSTRAINT FK_STUDENTS_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID)
);

CREATE TABLE COURSES (
    COURSE_ID       NUMBER(10) PRIMARY KEY,
    COURSE_NAME     VARCHAR2(100) NOT NULL,
    CREDITS         NUMBER(2),
    DEPT_ID         NUMBER(10),
    CONSTRAINT FK_COURSES_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID)
);

CREATE TABLE INSTRUCTORS (
    INSTRUCTOR_ID   NUMBER(10) PRIMARY KEY,
    FIRST_NAME      VARCHAR2(50) NOT NULL,
    LAST_NAME       VARCHAR2(50) NOT NULL,
    HIRE_DATE       DATE,
    DEPT_ID         NUMBER(10),
    CONSTRAINT FK_INSTRUCTORS_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID)
);
-- NOTE: INSTRUCTORS table EXISTS ONLY in DEV (not in PROD)

-- Level 3: Table with multiple FK dependencies
CREATE TABLE ENROLLMENTS (
    ENROLLMENT_ID   NUMBER(10) PRIMARY KEY,
    STUDENT_ID      NUMBER(10) NOT NULL,
    COURSE_ID       NUMBER(10) NOT NULL,
    ENROLLMENT_DATE DATE,
    GRADE           VARCHAR2(2),  -- COLUMN EXISTS ONLY IN DEV
    CONSTRAINT FK_ENROLLMENTS_STUDENT FOREIGN KEY (STUDENT_ID) 
        REFERENCES STUDENTS(STUDENT_ID),
    CONSTRAINT FK_ENROLLMENTS_COURSE FOREIGN KEY (COURSE_ID) 
        REFERENCES COURSES(COURSE_ID)
);

-- Insert test data for DEV_USER (respecting FK order)

-- Level 1 data
INSERT INTO DEPARTMENTS VALUES (1, 'Computer Science', 'Building A');
INSERT INTO DEPARTMENTS VALUES (2, 'Mathematics', 'Building B');
INSERT INTO DEPARTMENTS VALUES (3, 'Physics', 'Building C');

-- Level 2 data
INSERT INTO STUDENTS VALUES (101, 'Alice', 'Johnson', 'alice.j@university.edu', 1);
INSERT INTO STUDENTS VALUES (102, 'Bob', 'Smith', 'bob.s@university.edu', 1);
INSERT INTO STUDENTS VALUES (103, 'Carol', 'Williams', 'carol.w@university.edu', 2);

INSERT INTO COURSES VALUES (201, 'Database Systems', 4, 1);
INSERT INTO COURSES VALUES (202, 'Data Structures', 3, 1);
INSERT INTO COURSES VALUES (203, 'Linear Algebra', 4, 2);

INSERT INTO INSTRUCTORS VALUES (301, 'Dr. Emily', 'Davis', DATE '2015-09-01', 1);
INSERT INTO INSTRUCTORS VALUES (302, 'Prof. Michael', 'Brown', DATE '2010-01-15', 1);
INSERT INTO INSTRUCTORS VALUES (303, 'Dr. Sarah', 'Taylor', DATE '2018-08-20', 2);

-- Level 3 data
INSERT INTO ENROLLMENTS VALUES (401, 101, 201, DATE '2024-09-01', 'A');
INSERT INTO ENROLLMENTS VALUES (402, 101, 202, DATE '2024-09-01', 'B');
INSERT INTO ENROLLMENTS VALUES (403, 102, 201, DATE '2024-09-02', 'A');
INSERT INTO ENROLLMENTS VALUES (404, 103, 203, DATE '2024-09-01', 'A');

-- Indexes (exist only in DEV)
CREATE INDEX idx_students_email ON STUDENTS(EMAIL);

-- Procedures (exist only in DEV)
CREATE OR REPLACE PROCEDURE get_student_count(p_dept_id IN NUMBER, p_count OUT NUMBER) IS
BEGIN
    SELECT COUNT(*) INTO p_count FROM STUDENTS WHERE DEPT_ID = p_dept_id;
END;
/

-- Functions (exist only in DEV)
CREATE OR REPLACE FUNCTION get_student_email(p_student_id IN NUMBER) RETURN VARCHAR2 IS
    v_email VARCHAR2(100);
BEGIN
    SELECT EMAIL INTO v_email FROM STUDENTS WHERE STUDENT_ID = p_student_id;
    RETURN v_email;
END;
/

-- Package (exists only in DEV) - with unique functions not duplicating standalone
CREATE OR REPLACE PACKAGE university_pkg AS
    -- Подсчёт записей на курс
    FUNCTION get_enrollment_count(p_course_id IN NUMBER) RETURN NUMBER;
    -- Добавление нового департамента
    PROCEDURE add_department(p_id IN NUMBER, p_name IN VARCHAR2, p_building IN VARCHAR2);
END university_pkg;
/

CREATE OR REPLACE PACKAGE BODY university_pkg AS
    FUNCTION get_enrollment_count(p_course_id IN NUMBER) RETURN NUMBER IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count FROM ENROLLMENTS WHERE COURSE_ID = p_course_id;
        RETURN v_count;
    END;

    PROCEDURE add_department(p_id IN NUMBER, p_name IN VARCHAR2, p_building IN VARCHAR2) IS
    BEGIN
        INSERT INTO DEPARTMENTS VALUES (p_id, p_name, p_building);
        COMMIT;
    END;
END university_pkg;
/

COMMIT;

-- Verification query for DEV_USER
SELECT 'DEV_USER SETUP COMPLETE' AS STATUS FROM DUAL;
SELECT 'Tables created: ' || COUNT(*) AS TABLE_COUNT 
FROM USER_TABLES 
WHERE TABLE_NAME IN ('DEPARTMENTS', 'STUDENTS', 'COURSES', 'INSTRUCTORS', 'ENROLLMENTS');

SELECT object_name, object_type FROM user_objects
WHERE object_type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE');

SELECT object_name, object_type FROM all_procedures
WHERE owner = 'DEV_USER' AND object_type IN ('PROCEDURE', 'FUNCTION');

SELECT 'Procedures created: ' || COUNT(*) AS PROC_COUNT 
FROM USER_PROCEDURES WHERE OBJECT_TYPE = 'PROCEDURE';

SELECT 'Functions created: ' || COUNT(*) AS FUNC_COUNT 
FROM USER_PROCEDURES WHERE OBJECT_TYPE = 'FUNCTION';

SELECT 'Packages created: ' || COUNT(*) AS PKG_COUNT 
FROM USER_OBJECTS WHERE OBJECT_TYPE = 'PACKAGE';

SELECT 'Indexes created: ' || COUNT(*) AS IDX_COUNT 
FROM USER_INDEXES WHERE INDEX_NAME NOT LIKE 'SYS_%';


-------------------------------------------------------------------------------
-- SECTION 2: PROD_USER SCHEMA (MODIFIED VERSION)
-------------------------------------------------------------------------------
-- EXECUTE AS PROD_USER

-- Clean up existing objects (if re-running script)
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE ENROLLMENTS CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE STUDENTS CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE COURSES CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE DEPARTMENTS CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE OLD_TABLE (ID NUMBER);
CREATE INDEX idx_old ON OLD_TABLE(ID);
COMMIT;

-- Level 1: Base table (identical to DEV)
CREATE TABLE DEPARTMENTS (
    DEPT_ID         NUMBER(10) PRIMARY KEY,
    DEPT_NAME       VARCHAR2(100) NOT NULL,
    BUILDING        VARCHAR2(50)
);

-- Level 2: Tables with modifications
CREATE TABLE STUDENTS (
    STUDENT_ID      NUMBER(10) PRIMARY KEY,
    FIRST_NAME      VARCHAR2(50) NOT NULL,
    LAST_NAME       VARCHAR2(50) NOT NULL,
    -- EMAIL column MISSING in PROD (exists in DEV)
    DEPT_ID         NUMBER(10),
    CONSTRAINT FK_STUDENTS_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID)
);

CREATE TABLE COURSES (
    COURSE_ID       NUMBER(10) PRIMARY KEY,
    COURSE_NAME     VARCHAR2(100) NOT NULL,
    CREDITS         NUMBER(2),
    DEPT_ID         NUMBER(10),
    CONSTRAINT FK_COURSES_DEPT FOREIGN KEY (DEPT_ID) 
        REFERENCES DEPARTMENTS(DEPT_ID)
);

-- INSTRUCTORS table NOT CREATED in PROD (exists only in DEV)

-- Level 3: Table with modification
CREATE TABLE ENROLLMENTS (
    ENROLLMENT_ID   NUMBER(10) PRIMARY KEY,
    STUDENT_ID      NUMBER(10) NOT NULL,
    COURSE_ID       NUMBER(10) NOT NULL,
    ENROLLMENT_DATE DATE,
    -- GRADE column MISSING in PROD (exists in DEV)
    CONSTRAINT FK_ENROLLMENTS_STUDENT FOREIGN KEY (STUDENT_ID) 
        REFERENCES STUDENTS(STUDENT_ID),
    CONSTRAINT FK_ENROLLMENTS_COURSE FOREIGN KEY (COURSE_ID) 
        REFERENCES COURSES(COURSE_ID)
);

-- Insert test data for PROD_USER (respecting FK order)

-- Level 1 data (identical to DEV)
INSERT INTO DEPARTMENTS VALUES (1, 'Computer Science', 'Building A');
INSERT INTO DEPARTMENTS VALUES (2, 'Mathematics', 'Building B');
INSERT INTO DEPARTMENTS VALUES (3, 'Physics', 'Building C');

-- Level 2 data (without EMAIL column)
INSERT INTO STUDENTS VALUES (101, 'Alice', 'Johnson', 1);
INSERT INTO STUDENTS VALUES (102, 'Bob', 'Smith', 1);
INSERT INTO STUDENTS VALUES (103, 'Carol', 'Williams', 2);

INSERT INTO COURSES VALUES (201, 'Database Systems', 4, 1);
INSERT INTO COURSES VALUES (202, 'Data Structures', 3, 1);
INSERT INTO COURSES VALUES (203, 'Linear Algebra', 4, 2);

-- NO INSTRUCTORS DATA (table doesn't exist in PROD)

-- Level 3 data (without GRADE column)
INSERT INTO ENROLLMENTS VALUES (401, 101, 201, DATE '2024-09-01');
INSERT INTO ENROLLMENTS VALUES (402, 101, 202, DATE '2024-09-01');
INSERT INTO ENROLLMENTS VALUES (403, 102, 201, DATE '2024-09-02');
INSERT INTO ENROLLMENTS VALUES (404, 103, 203, DATE '2024-09-01');

COMMIT;

-- Verification query for PROD_USER
SELECT 'PROD_USER SETUP COMPLETE' AS STATUS FROM DUAL;
SELECT 'Tables created: ' || COUNT(*) AS TABLE_COUNT 
FROM USER_TABLES 
WHERE TABLE_NAME IN ('DEPARTMENTS', 'STUDENTS', 'COURSES', 'ENROLLMENTS');


-------------------------------------------------------------------------------
-- SECTION 3: VERIFICATION QUERIES
-------------------------------------------------------------------------------

/*
-- Run as DEV_USER to verify schema:
SELECT table_name, column_name, data_type 
FROM user_tab_columns 
WHERE table_name IN ('DEPARTMENTS', 'STUDENTS', 'COURSES', 'INSTRUCTORS', 'ENROLLMENTS')
ORDER BY table_name, column_id;

SELECT constraint_name, table_name, constraint_type 
FROM user_constraints 
WHERE table_name IN ('DEPARTMENTS', 'STUDENTS', 'COURSES', 'INSTRUCTORS', 'ENROLLMENTS')
ORDER BY table_name;

SELECT COUNT(*) AS TOTAL_ROWS FROM DEPARTMENTS;
SELECT COUNT(*) AS TOTAL_ROWS FROM STUDENTS;
SELECT COUNT(*) AS TOTAL_ROWS FROM COURSES;
SELECT COUNT(*) AS TOTAL_ROWS FROM INSTRUCTORS;
SELECT COUNT(*) AS TOTAL_ROWS FROM ENROLLMENTS;
*/

/*
-- Run as PROD_USER to verify schema:
SELECT table_name, column_name, data_type 
FROM user_tab_columns 
WHERE table_name IN ('DEPARTMENTS', 'STUDENTS', 'COURSES', 'ENROLLMENTS')
ORDER BY table_name, column_id;

SELECT constraint_name, table_name, constraint_type 
FROM user_constraints 
WHERE table_name IN ('DEPARTMENTS', 'STUDENTS', 'COURSES', 'ENROLLMENTS')
ORDER BY table_name;

SELECT COUNT(*) AS TOTAL_ROWS FROM DEPARTMENTS;
SELECT COUNT(*) AS TOTAL_ROWS FROM STUDENTS;
SELECT COUNT(*) AS TOTAL_ROWS FROM COURSES;
SELECT COUNT(*) AS TOTAL_ROWS FROM ENROLLMENTS;
*/

/*
-- Expected schema differences for testing:
-- 1. TABLE MISSING: INSTRUCTORS exists in DEV but not in PROD
-- 2. DIFFERENT (column level):
--    - STUDENTS.EMAIL exists in DEV, missing in PROD
--    - ENROLLMENTS.GRADE exists in DEV, missing in PROD
-- 3. PROCEDURE MISSING: GET_STUDENT_COUNT, ADD_STUDENT exist only in DEV
-- 4. FUNCTION MISSING: GET_DEPT_NAME, GET_STUDENT_EMAIL exist only in DEV
-- 5. INDEX MISSING: IDX_STUDENTS_NAME, IDX_STUDENTS_EMAIL exist only in DEV
-- 6. PACKAGE MISSING: UNIVERSITY_PKG exists only in DEV
--
-- To test comparison function (as SYSTEM):
-- SELECT obj_type, obj_name, status, details, sort_order 
-- FROM TABLE(compare_schemas_oracle('DEV_USER', 'PROD_USER'))
-- ORDER BY sort_order;
*/
