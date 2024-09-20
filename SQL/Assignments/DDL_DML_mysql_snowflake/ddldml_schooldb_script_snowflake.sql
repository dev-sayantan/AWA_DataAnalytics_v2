-- USE ROLE accountadmin;
-- USE WAREHOUSE compute_wh;

CREATE DATABASE IF NOT EXISTS SchoolDB;
CREATE SCHEMA IF NOT EXISTS SCHOOLDB.School_Schema;
-- DROP DATABASE schooldb;
-- CREATING FILE FORMAT of SchoolDB_Schema SchoolDB Database   
create or replace file format school_csv
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ','
    field_optionally_enclosed_by = '\042'
    error_on_column_count_mismatch=false,
    skip_header = 1 ;
    
CREATE OR REPLACE STORAGE integration school_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::664418982553:role/School_Role'
STORAGE_ALLOWED_LOCATIONS =('s3://myddldml/');

DESC integration school_integration;
SHOW STORAGE INTEGRATIONS;

CREATE OR REPLACE STAGE SCHOOL_STAGE
URL = 's3://myddldml'
file_format = school_csv
storage_integration = school_integration;

LIST @SCHOOL_STAGE;
DESCRIBE STAGE SCHOOL_STAGE;
SHOW STAGES;

CREATE OR REPLACE PIPE SCHOOL_SNOWPIPE_STU AUTO_INGEST = TRUE 
AS 
COPY INTO "SCHOOLDB"."SCHOOL_SCHEMA"."STUDENTS"
FROM '@SCHOOL_STAGE/students/'
FILE_FORMAT = school_csv;

DESCRIBE PIPE SCHOOL_SNOWPIPE_STU;
SHOW PIPES;


ALTER PIPE SCHOOL_SNOWPIPE_STU refresh;

SELECT count(*) FROM STUDENTS;

SELECT * FROM STUDENTS;

-- select *
--   from table (information_schema.pipe_usage_history(
--     date_range_start=>to_timestamp_tz('2024-09-08 11:59:00.000 +0530'),
--     date_range_end=>to_timestamp_tz('2024-09-09 02:05:00.000 +0530')));

select * from table(information_schema.copy_history(table_name=>'STUDENTS', 
start_time=> dateadd(hours, -8, current_timestamp())));
---------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE SCHOOLDB.SCHOOL_SCHEMA.Students (
    StudentID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DateOfBirth DATE,
    EnrolmentDate DATE,
    Major VARCHAR(50)
);

-- Enforce Referential Integrity:
-- Add a foreign key constraint to the Students table, linking the Major column to a hypothetical Majors table that contains all valid majors offered by the school.

SELECT DISTINCT(SCHOOLDB.SCHOOL_SCHEMA.STUDENTS.Course) FROM STUDENTS;

INSERT INTO SCHOOLDB.SCHOOL_SCHEMA.MAJORS(StudentID, Course) 
VALUES((1,'Engineering'),(2,'Mathematics'),(3,'Art'),(4,'Science'),(5,'Computer Science'),(6,'History'),(7,'Alumni'));

ALTER TABLE SCHOOLDB.SCHOOL_SCHEMA.MAJORS
ADD CONSTRAINT PK_Student
PRIMARY KEY(StudentID);

ALTER TABLE Students
MODIFY COLUMN StudentID
Number(8,0);

ALTER TABLE SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
ADD CONSTRAINT FK_Students_Major
FOREIGN KEY(StudentID) REFERENCES Majors(StudentID);

-- Ensure Valid Date Values:
-- Add a check constraint to the Students table to ensure that the EnrollmentDate is not earlier than 2000-01-01.

-- ALTER TABLE SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
-- ADD CONSTRAINT CC_EnrolmentDate
-- CHECK (EnrolmentDate >= to_date('2000-01-01','YYYY-MM-DD'));

-- Composite Keys and Indexes
-- Composite Primary Key:
-- Create a composite primary key on the combination of FirstName and LastName in the Students table (assuming StudentID is no longer the primary key).

ALTER TABLE SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
DROP PRIMARY KEY;

ALTER TABLE SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
ADD CONSTRAINT pk_student_fullname
PRIMARY KEY (FirstName, LastName);

-- Rename Columns:
-- Rename the Major column in the Students table to Course to better reflect the data it stores.

ALTER TABLE SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
RENAME COLUMN Major TO Course;

-- Change the default value for the EnrollmentDate in the Students table to the current date.

-- ALTER TABLE SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
-- ALTER COLUMN EnrolmentDate SET DEFAULT CURRENT_DATE();

-- For all students who enrolled before 2020-01-01, change their Course to 'Alumni'.
UPDATE SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
SET Course = 'Alumni'
WHERE EnrolmentDate < '2020-01-01';

-- Add and Drop Indexes:
-- Add an index on the EnrollmentDate column in the Students table to speed up queries.
CREATE OR REPLACE INDEX idx ON SCHOOLDB.SCHOOL_SCHEMA.STUDENTS (EnrollmentDate);

-- Add a column Graduated (BOOLEAN) to the Students table, then update this column to TRUE for all students whose Course is 'Alumni'.
ALTER TABLE SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
ADD COLUMN Graduated BOOLEAN;
UPDATE SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
SET Graduated = True
WHERE Course = 'Alumni';

-- Remove the student from the Students table whose StudentID is 3.
DELETE FROM SCHOOLDB.SCHOOL_SCHEMA.STUDENTS WHERE StudentID = 3;

-- Remove all students from the Students table who enrolled before 2019-01-01.
DELETE FROM SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
WHERE EnrolmentDate < '2019-01-01';
-- Remove all students from the Students table whose Course is 'History' and who enrolled before 2021-01-01.
DELETE FROM SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
WHERE Course = 'History' AND EnrolmentDate < '2021-01-01';
-- Remove all students from the Students table if there is no record of their major in a hypothetical Majors table.
DELETE FROM SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
USING MAJORS
WHERE Students.StudentID = Majors.StudentID
AND
Majors.Course = NULL;

-- DELETE with Cascading and Foreign Keys
-- Cascade DELETE Operations:
-- Remove a course from the Courses table (hypothetical) and ensure all students enrolled in that course are also removed.

CREATE OR REPLACE TABLE SCHOOLDB.SCHOOL_SCHEMA.Courses(
    CourseID NUMBER(8,0) NOT NULL PRIMARY KEY,
    Course VARCHAR(50) NOT NULL REFERENCES Students(Course) ON DELETE CASCADE
);

INSERT INTO SCHOOLDB.SCHOOL_SCHEMA.Courses(CourseID, Course)
VALUES (1,'Engineering'),
        (2,'Mathematics'),
        (3,'Art'),
        (4,'Science'),
        (5,'Computer Science'),
        (6,'History'),
        (7,'Alumni');

DELETE FROM Students WHERE Course = 'History';

-- Delete Using a Restriction:
-- Attempt to delete a student from the Students table if they are referenced in another table, such as a Grades table (hypothetical).

CREATE OR REPLACE TABLE SCHOOLDB.SCHOOL_SCHEMA.Grades(
    StudentID NUMBER(8,0) NOT NULL PRIMARY KEY,
    StudentFirstName VARCHAR(100) NOT NULL,
    StudentLastName VARCHAR(100) NOT NULL,
    Course VARCHAR(50) NOT NULL,
    GradePoints NUMBER(4) NOT NULL
);

ALTER TABLE SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
ADD CONSTRAINT FK_StudentID_Grade
FOREIGN KEY(StudentID) REFERENCES Grades(StudentID)
ON DELETE CASCADE;

-- Copying data from columns of Students table
CREATE OR REPLACE TABLE SCHOOLDB.SCHOOL_SCHEMA.Grades
AS
SELECT StudentID, FirstName, LastName, Course
FROM Students;

-- Adding a new column in the Grades Table --> GRADES, having not null constraint set with a default 'F' grade denotes as Fail..
ALTER TABLE GRADES
ADD COLUMN Grades VARCHAR(2) NOT NULL DEFAULT 'F';

-- Using a built-in function RANDSTR(), Updating default grades from 'F' to 'C'
UPDATE GRADES
SET Grades = RANDSTR(1,1);

DELETE FROM STUDENTS WHERE STUDENTID = 41;

-- Deleting All Records with a Condition
-- Delete All Records Based on a Common Attribute:
-- Remove all records from the Students table where the Course is 'Art'.

DELETE FROM Students
Where Course = 'Art';

-- Delete Records with NULL Values:
-- Remove all students from the Students table where the EnrollmentDate is NULL.

DELETE FROM SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
WHERE EnrolmentDate IS NULL;

-- Conditional DELETE Using Aggregate Functions:
-- Remove students from the Students table who enrolled after the latest enrollment date in the table.

DELETE FROM SCHOOLDB.SCHOOL_SCHEMA.STUDENTS
WHERE EnrolmentDate > (SELECT MAX(EnrolmentDate) FROM SCHOOLDB.SCHOOL_SCHEMA.STUDENTS);