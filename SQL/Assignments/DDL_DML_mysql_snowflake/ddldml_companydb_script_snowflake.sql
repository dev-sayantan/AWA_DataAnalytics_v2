-- USE ROLE accountadmin;
-- USE WAREHOUSE compute_wh;

-- SELECT CURRENT_ACCOUNT();

CREATE DATABASE IF NOT EXISTS CompanyDB;
CREATE SCHEMA IF NOT EXISTS COMPANYDB.Company_Schema;

-- CREATING FILE FORMAT of Company_Schema of CompanyDB Database
create or replace file format company_csv
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ','
    field_optionally_enclosed_by = '\042',
    skip_header = 1 ;

-- creating STORAGE INTEGRATION
CREATE OR REPLACE STORAGE integration company_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::664418982553:role/Company_Role'
STORAGE_ALLOWED_LOCATIONS =('s3://myddldml/');

DESC integration company_integration;
-- SHOW STORAGE INTEGRATIONS;


CREATE OR REPLACE STAGE COMPANY_STAGE
URL = 's3://myddldml'
file_format = company_csv
storage_integration = company_integration;

-- alter STAGE COMPANY_STAGE set DIRECTORY = ( ENABLE = true AUTO_REFRESH = true );
-- alter STAGE COMPANY_STAGE refresh;

LIST @COMPANY_STAGE;
DESC STAGE COMPANY_STAGE;
-- SHOW STAGES;

--CREATE SNOWPIPE THAT RECOGNISES CSV THAT ARE INGESTED FROM EXTERNAL STAGE AND COPIES THE DATA INTO EXISTING TABLE
--The AUTO_INGEST=true parameter specifies to read event notifications sent from an S3 bucket to an SQS queue when new data is ready to load.

CREATE OR REPLACE PIPE COMP_SNOWPIPE_DEPT AUTO_INGEST = TRUE 
AS 
COPY INTO "COMPANYDB"."COMPANY_SCHEMA"."DEPARTMENTS"
FROM '@COMPANY_STAGE/DEPARTMENTS/'
FILE_FORMAT = company_csv;

CREATE OR REPLACE PIPE COMP_SNOWPIPE_EMPL AUTO_INGEST = TRUE 
AS 
COPY INTO "COMPANYDB"."COMPANY_SCHEMA"."EMPLOYEES"
FROM '@COMPANY_STAGE/EMPLOYEES/'
FILE_FORMAT = company_csv;

SHOW PIPES;

ALTER PIPE COMP_SNOWPIPE_DEPT refresh;
ALTER PIPE COMP_SNOWPIPE_EMPL refresh;

-- SELECT count(*) FROM DEPARTMENTS;
-- SELECT count(*) FROM EMPLOYEES;

-- SELECT * FROM DEPARTMENTS;
-- SELECT * FROM EMPLOYEES;

select SYSTEM$PIPE_STATUS('COMP_SNOWPIPE_EMPL');
select SYSTEM$PIPE_STATUS('COMP_SNOWPIPE_DEPT');

-- select *
--   from table(information_schema.pipe_usage_history(
--     date_range_start=>dateadd('day',-14,current_date()),
--     date_range_end=>current_date()));

SELECT * FROM table(information_schema.pipe_usage_history());

-- select *
--   from table (information_schema.pipe_usage_history(
--     date_range_start=>to_timestamp_tz('2024-09-08 11:59:00.000 +0530'),
--     date_range_end=>to_timestamp_tz('2024-09-09 02:05:00.000 +0530')));

-- SELECT * FROM TABLE(
--     INFORMATION_SCHEMA.COPY_HISTORY(
--         TABLE_NAME => 'EMPLOYEES'
--         ,START_TIME => DATEADD(days, -7, CURRENT_TIMESTAMP())
--     )
-- );

-- select * from table(information_schema.copy_history(table_name=>'EMPLOYEES', 
-- start_time=> dateadd(hours, -8, current_timestamp())));

-- select * from table(information_schema.copy_history(table_name=>'DEPARTMENTS', 
-- start_time=> dateadd(hours, -8, current_timestamp())));
------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE COMPANYDB.COMPANY_SCHEMA.Employees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DateOfBirth DATE,
    JobTitle VARCHAR(50),
    Salary DECIMAL(10,2),
    DepartmentID INT,
    Email VARCHAR(50)
);

select * from COMPANYDB.COMPANY_SCHEMA.Employees;

CREATE OR REPLACE TABLE COMPANYDB.COMPANY_SCHEMA.Departments (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(100)
);

select * from COMPANYDB.COMPANY_SCHEMA.Departments;

ALTER TABLE IF EXISTS COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD COLUMN IF NOT EXISTS HireDate DATE;

ALTER TABLE IF EXISTS COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS 
MODIFY COLUMN DEPARTMENTNAME 
SET DATA TYPE VARCHAR(150);


-- 2.Data Insertion, Updating, and Deletion (DML)
-- 1.1 Insert Data:
-- 1.2 Insert five records into the Employees table.

-- select count(*) from employees;
INSERT INTO COMPANYDB.COMPANY_SCHEMA.EMPLOYEES(
    EMPLOYEEID, 
    FIRSTNAME, 
    LASTNAME, 
    DATEOFBIRTH, 
    JOBTITLE, 
    SALARY, 
    DEPARTMENTID, 
    EMAIL, 
    HIREDATE) VALUES (1001,'Sayantan','Naha',TO_DATE('1989-11-16','YYYY-MM-DD'),'Intern',15000.76,3,'sayantannaha77@gmail.com',TO_DATE('2024-08-15','YYYY-MM-DD')),
                    (1002,'Sandip','Saha',TO_DATE('1988-09-16','YYYY-MM-DD'),'Consultant',50000.75,3,'sandip.saha@gmail.com',TO_DATE('2017-10-27','YYYY-MM-DD')),
                    (1003,'Samit','Dutta',TO_DATE('1992-07-07','YYYY-MM-DD'),'Sales Representative',25000.45,2,'sumit.dutta@gmail.com',TO_DATE('2024-01-25','YYYY-MM-DD')),
                    (1004,'Subhajit','Ghosh',TO_DATE('1990-12-09','YYYY-MM-DD'),'HR Manager',48000.59,1,'subhajit.ghosh@gmail.com',TO_DATE('2020-10-15','YYYY-MM-DD')),
                    (1005,'Sayak','Dutta',TO_DATE('1993-05-15','YYYY-MM-DD'),'Intern',15000.76,4,'sayak.dutta@gmail.com',TO_DATE('2024-07-16','YYYY-MM-DD'));
                  
-- Insert three records into the Departments table.

-- select count(*) from departments;
INSERT INTO COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS(
    DEPARTMENTID, 
    DEPARTMENTNAME) VALUES (6,'Operations'),(7,'Supply Chain'),(8,'Finance');
INSERT INTO COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS(
    DEPARTMENTID, 
    DEPARTMENTNAME) VALUES (4,'Production & Manufacturing');
    
-- 2 Update Data:
-- 2.1 Update the Salary of the employee with EmployeeID = 3 to 75000.
-- 2.2 Update the Position of all employees where JobTitle is Intern to Junior Developer.

UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
    SET Salary = 75000
    WHERE EmployeeID = 3;

UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
SET JobTitle = 'Junior Developer'
WHERE JobTitle = 'Intern';

-- 3. Delete Data:
-- 3.1 Delete the employee record with EmployeeID = 4.
-- 3.2 Delete all records from the Departments table where the DepartmentName is HR.

DELETE FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
WHERE EmployeeID = 4;

DELETE FROM COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
WHERE DepartmentName = 'HR';

-- 4. Complex Insert and Update:
-- 4.1 Insert a new employee, ensuring that their DepartmentID exists in the Departments table.
-- 4.2 Update all employees who have NULL in the HireDate column to the current date.
-- select * from employees;
INSERT INTO COMPANYDB.COMPANY_SCHEMA.EMPLOYEES(
    EMPLOYEEID, 
    FIRSTNAME, 
    LASTNAME, 
    DATEOFBIRTH, 
    JOBTITLE, 
    SALARY, 
    DEPARTMENTID, 
    EMAIL, 
    HIREDATE) 
    SELECT 1006,
        'Jayanta',
        'Sen',
        TO_DATE('1991-11-16','YYYY-MM-DD'),
        'Intern',
        15003.76,
        4,
        'jayanta.sen77@gmail.com',
        TO_DATE('2024-08-15','YYYY-MM-DD') 
    WHERE EXISTS (
        SELECT DEPARTMENTID FROM COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
        )
;

UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
SET HIREDATE = CURRENT_DATE()
WHERE HIREDATE IS NULL;

-- 3. Data Selection and Filtering (DML)
-- 1. Select Data:
-- 1.1 Select all columns from the Employees table.
-- 1.2 Select the FirstName, LastName, and Salary of all employees who have a salary greater than 60000.

SELECT * FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES;
SELECT FirstName, LastName, Salary FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES WHERE Salary > 60000;

-- 2. Filtering and Sorting:
-- 2.1 Select all employees from the Employees table who were hired after 2018-01-01.
-- 2.2 Select all employees from the Employees table and order them by LastName in ascending order.

SELECT * FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES WHERE HIREDATE > '2018-01-01';

SELECT * FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ORDER BY LASTNAME ASC;

-- 3. Aggregate Functions:
-- 3.1 Count the total number of employees in the Employees table.
-- 3.2 Calculate the average Salary of all employees.

SELECT COUNT(*) FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES;
SELECT AVG(SALARY) FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES;

-- 4. Primary Key and Foreign Key Constraints
-- 4.1 Enforce Uniqueness:
-- 4.1.1 Ensure that the EmployeeID in the Employees table is unique and cannot be NULL.
-- 4.1.2 Ensure that each department in the Departments table has a unique DepartmentID that is also not NULL.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD CONSTRAINT PK_EmployeeID PRIMARY KEY (EmployeeID);

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
ADD CONSTRAINT PK_DepartmentID PRIMARY KEY (DepartmentID);

-- 4.2 Establish Relationships:
-- 4.2.1 Modify the Employees table to add a DepartmentID column (if not already present) and create a foreign key relationship between the Employees table and the Departments table on DepartmentID.
-- 4.2.2 Ensure that the DepartmentID in the Employees table cannot have a value that does not exist in the Departments table.

ALTER TABLE IF EXISTS COMPANYDB.COMPANY_SCHEMA.EMPLOYEES 
ADD COLUMN IF NOT EXISTS
DepartmentID INT;

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD CONSTRAINT FK_Employee_Department
FOREIGN KEY (DepartmentID) references COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS (DepartmentID);

-- 4.3. Cascade on Delete:
-- 4.3.1 Modify the foreign key in the Employees table so that if a department is deleted from the Departments table, all employees associated with that department are also deleted.
ALTER TABLE EMPLOYEES
DROP CONSTRAINT FK_EMPLOYEE_DEPARTMENT;

ALTER TABLE EMPLOYEES
ADD CONSTRAINT FK_Employees_Departments 
FOREIGN KEY(DepartmentID) 
references Departments(DepartmentID) ON DELETE CASCADE;

-- 4.4 Enforce Referential Integrity:
-- Unique and Not Null Constraints
-- 1. Ensure Unique Values:
-- Add a unique constraint on the FirstName and LastName combination in the Employees table, ensuring that no two employees can have the same first and last name combination.
-- Ensure that the DepartmentName in the Departments table is unique.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD CONSTRAINT UC_Employee_FullName UNIQUE (FirstName,LastName);

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
ADD CONSTRAINT UC_DeptName UNIQUE (DepartmentName);

-- 2. Prevent NULL Values:
-- Modify the Employees table to ensure that the FirstName, LastName, DateOfBirth, and Salary columns cannot contain NULL values.
-- In the Departments table, ensure that the DepartmentName cannot be NULL.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
MODIFY COLUMN FirstName 
SET NOT NULL;

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
MODIFY COLUMN LastName 
SET NOT NULL;        

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
MODIFY COLUMN DateOfBirth 
SET NOT NULL;

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
MODIFY COLUMN Salary 
SET NOT NULL;

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
MODIFY COLUMN DepartmentName 
SET NOT NULL;

-- Default and Check Constraints
-- Set Default Values:
-- 1. Add a default value of 'Unknown' to the Position column in the Employees table, so if no position is specified, it will default to 'Unknown'.
-- 2. Set a default value of '1000' for the Salary column in the Employees table.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD COLUMN JobTitles VARCHAR(100) DEFAULT 'Unknown';

UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
SET JobTitles = JobTitle;

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
DROP COLUMN JobTitle;

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD COLUMN Salaries DECIMAL(10,2) DEFAULT 1000;

UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
SET Salaries = Salary;

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
DROP COLUMN Salary;

-- Enforce Valid Data Ranges with CHECK:
-- 1. Add a check constraint to the Employees table that ensures the Salary is greater than 0.
-- 2. Add a check constraint to the Departments table to ensure that the DepartmentName is at least 3 characters long.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD CHECK(Salary > 0);

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
ADD CONSTRAINT CC_DeptName
CHECK (LENGTH(DepartmentName) >= 3);

-- Ensure Valid Date Values:
-- 1. Add a check constraint to the Employees table to ensure that the HireDate is not in the future.
-- 2. Add a check constraint to the Students table to ensure that the EnrollmentDate is not earlier than 2000-01-01.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD CONSTRAINT CC_HireDate
CHECK (HireDate <= CURRENT_DATE);

-- 2. In the Employees table, create a composite primary key on the combination of EmployeeID and HireDate (if HireDate is unique for each employee).
ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
DROP PRIMARY KEY;
ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD CONSTRAINT pk_EmpID_HireDate
PRIMARY KEY (EmployeeID, HireDate);

-- Composite Unique Key:
-- 1. Add a composite unique key on the combination of FirstName and LastName in the Departments table to ensure no two departments can have the same head's first and last name combination.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
ADD COLUMN IF NOT EXISTS HeadFirstName VARCHAR(50), HeadLastName VARCHAR(50);

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
ADD CONSTRAINT CUK_Head_FullName
UNIQUE (HeadFirstName, HeadLastName);

-- Practice Questions on UPDATE, ALTER, and MODIFY Commands
-- 1. Altering and Modifying Table Structures
-- 1. Modify Data Types:
-- Change the Salary column in the Employees table to a larger precision, for example, DECIMAL(12,2) to allow for higher salaries.
-- Modify the DateOfBirth column in the Employees table from DATE to DATETIME to include time of birth.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
MODIFY COLUMN Salaries DECIMAL(12,2);

UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
SET DateOfBirth = DateOfBirth::TIMESTAMP_NTZ;

-- 2. Add New Columns:
-- Add a new column Email (VARCHAR(100)) to the Employees table to store employee email addresses.
-- Add a new column DepartmentHead (BOOLEAN) to the Departments table to indicate if a department has a head.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD COLUMN IF NOT EXISTS Email VARCHAR(100);

-- ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
-- MODIFY COLUMN Email VARCHAR(100);

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
ADD COLUMN IF NOT EXISTS DepartmentHead BOOLEAN;

-- ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
-- RENAME COLUMN Position TO JobTitle;

-- 4. Remove Columns:
-- Drop the HireDate column from the Employees table as it is no longer needed.

-- ALTER TABLE COMPANYDB.COMPANYDB_SCHEMA.EMPLOYEES
-- DROP COLUMN IF EXISTS HireDate;

-- Remove the DepartmentHead column from the Departments table.

-- ALTER TABLE COMPANYDB.COMPANYDB_SCHEMA.DEPARTMENTS
-- DROP COLUMN IF EXISTS DepartmentHead;

-- 5. Change Default Values:
-- Modify the default value for the Position column in the Employees table to 'Employee' instead of 'Unknown'.

-- ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
-- MODIFY JobTitles SET DEFAULT 'Employee';

-- 2. Updating Data
-- 1.Basic Updates:
-- Update the Salary of all employees with the Position of 'Junior Developer' to 70000.

UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
SET Salaries = 70000
WHERE JobTitles = 'Junior Developer';

-- Change the DepartmentName of the department with DepartmentID = 2 to 'Research & Development'.

UPDATE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
SET DepartmentName = 'Research & Development'
Where DepartmentID = 2;

-- 2. Conditional Updates:
-- Update the JobTitle of employees who were hired before 2015-01-01 to 'Senior Developer'.

UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
SET JobTitles = 'Senior Developer'
WHERE Hiredate < '2015-01-01';

-- 3. Bulk Updates:
-- Increase the Salary of all employees by 10%.
UPDATE employees SET salaries = salaries + salaries*0.1;
-- Set the DepartmentID of all employees currently in the HR department to NULL (assuming the department is being dissolved).
UPDATE employees SET departmentid = NULL
WHERE departmentid = (
    SELECT departmentid FROM departments 
    WHERE departmentname = 'HR'
);
-- 4. Update Using Joins:
-- Update the Salary of employees in the Sales department to 80000 using a join between Employees and Departments on DepartmentID.
-- Set the DepartmentHead to TRUE for the department that has an employee named 'John Doe'.

UPDATE employees
SET employees.salaries = 80000
FROM departments
WHERE employees.departmentid = departments.departmentid
AND departments.departmentname = 'Sales';

UPDATE departments
SET departmenthead = TRUE
FROM employees
WHERE employees.departmentid = departments.departmentid
AND employees.firstname = 'John' AND employees.lastname = 'Doe';

-- 3.Advanced Table Modifications
-- Reorganize Table:
-- Change the order of columns in the Employees table to have LastName appear before FirstName.

CREATE TABLE employees1
AS
SELECT EMPLOYEEID, LASTNAME, FIRSTNAME, DATEOFBIRTH, JOBTITLES, SALARIES, DEPARTMENTID, EMAIL, HIREDATE
FROM employees;

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES1
RENAME TO empl;

-- CREATE OR REPLACE TABLE employees
-- AS
-- SELECT EMPLOYEEID, LASTNAME, FIRSTNAME, DATEOFBIRTH, JOBTITLES, SALARIES, DEPARTMENTID, EMAIL, HIREDATE
-- FROM employees;

-- Reorder the Departments table so that DepartmentID is the last column.

CREATE OR REPLACE TABLE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS 
AS
SELECT DEPARTMENTNAME, HEADFIRSTNAME, HEADLASTNAME, DEPARTMENTHEAD, DEPARTMENTID
FROM departments;

-- Drop and Add Constraints:
-- Drop the foreign key constraint on DepartmentID in the Employees table, and then add it back with ON DELETE CASCADE.
ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
DROP CONSTRAINT FK_Employee_Department;

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD CONSTRAINT FK_Employee_Department
FOREIGN KEY (DepartmentID)
references COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS (DepartmentID) ON DELETE CASCADE;

-- Drop the unique constraint on the combination of FirstName and LastName in the Employees table.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
DROP CONSTRAINT UC_EMPLOYEE_FULLNAME;

-- Add and Drop Indexes:
-- Drop the composite index on FirstName and LastName in the Employees table.


-- Combined Operations
-- Update and Alter Combined:
-- First, update all employee Salaries to NULL, then modify the Salary column to set a NOT NULL constraint with a default value of 50000.

-- Because previously Salary Column had a NOT NULL constraint already and snowflake doesn't allow to set null values to a not null column..
ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
MODIFY COLUMN Salaries
DROP NOT NULL;
-- Now it's allowed to update the Salary column values to NULL
UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
SET Salaries = NULL;
-- Because snowflake doesn't allow to apply NOT NULL directly when there are existing NULL values, 
-- first it is needed to set value of Salary column to 50000..
UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
SET Salaries = 50000
WHERE Salaries IS NULL;
-- Then will set NOT NULL constraint for the Salary Column..
ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
MODIFY COLUMN Salaries
SET NOT NULL;

-- Also, will set Salary Column with the default value of 50000..
-- Because, I didn't create a sequence before, and applied the sequence during creation of the table EMPLOYEES,
-- for that reason,ALTER COLUMN..SET DEFAULT is not working.. producing this error - 000002 (0A000): Unsupported feature 'Alter Column Set Default'.
-- REFERENCE DOC --> https://community.snowflake.com/s/article/Error-when-setting-default-sequence-Unsupported-feature-Alter-Column-Set-Default

-- ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
-- MODIFY COLUMN Salaries
-- SET DEFAULT 50000;

-- Add a new column PhoneNumber to the Employees table and immediately populate it with a default value for all existing rows.

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD COLUMN PhoneNumber Number(12,0) DEFAULT 91;

-- Modify and Update Combined:
-- Modify the JobTitle column to accept a maximum of 100 characters, then update all employees with the title 'Intern' to have the title 'Temporary Employee'.
ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
MODIFY JobTitles VARCHAR(100);

UPDATE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
SET Jobtitles = 'Temporary Employee'
WHERE JobTitles = 'Intern';

-- Practice Questions on DELETE Commands with Conditions
-- Basic DELETE Operations
-- Delete a Specific Record:
-- Delete the employee from the Employees table where the EmployeeID is 5.

DELETE FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES WHERE EMPLOYEEID = 5;

-- Delete Multiple Records Based on a Condition:
-- Delete all employees from the Employees table who have a Salary less than 50000.

DELETE FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES WHERE Salaries < 50000;

-- DELETE with Complex Conditions
-- Delete Using AND/OR Conditions:
-- Delete all employees from the Employees table who are either in the HR department or have a JobTitle of 'Intern'.

-- Because, in a previous query, departmentname HR was deleted, below I inserted one record 'HR' into departmentname column in departments table..
INSERT INTO 
COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS(DepartmentName) 
VALUES ('HR');
UPDATE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
SET DepartmentID = 1
WHERE DepartmentName = 'HR';
-- Now, Using AND/OR conditions..
DELETE FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
USING COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
WHERE employees.departmentid = departments.departmentid
AND employees.jobtitles = 'Intern' OR employees.departmentid = 1;

-- select departmentid from employees where departmentid = 1;
-- select jobtitles from employees where jobtitles = 'intern';

-- Delete Using EXISTS Clause:
-- Delete all employees from the Employees table where the department they belong to no longer exists in the Departments table.

DELETE FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
WHERE NOT EXISTS (SELECT 1 FROM COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS 
                  WHERE departments.departmentid = employees.departmentid);
-- select distinct departmentid from employees;

-- DELETE with Cascading and Foreign Keys
-- Cascade DELETE Operations:
-- Delete a department from the Departments table and ensure all related employees are also deleted (requires foreign key with ON DELETE CASCADE).

-- INSERT INTO 
-- COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS(DepartmentName) 
-- VALUES ('Research & Development');
-- UPDATE COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
-- SET DepartmentID = 2
-- WHERE DepartmentName = 'Research & Development';
ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
DROP CONSTRAINT FK_Employees_Departments;

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
ADD CONSTRAINT FK_Employees_Departments
FOREIGN KEY (DepartmentID) references COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS(DepartmentID)
ON DELETE CASCADE;

DELETE FROM COMPANYDB.COMPANY_SCHEMA.DEPARTMENTS
WHERE departmentname = 'Operations'
AND departmentid = 6;

-- Delete Using a Restriction:
-- Attempt to delete a department from the Departments table where there are still employees assigned to that department, and observe what happens (if foreign keys are not set to cascade).

-- Experimentation, If foreign keys are not set to cascade. Before deleting a DepartmentName from DEPT table, we need to set a primary key on DepartmentID column on DEPT table and then will setup the foreign key.
ALTER TABLE COMPANYDB.COMPANY_SCHEMA.DEPT
ADD CONSTRAINT PK_DEPTID
PRIMARY KEY(DepartmentID);

ALTER TABLE COMPANYDB.COMPANY_SCHEMA.EMPL
ADD CONSTRAINT FK_EMPL_DEPTID
FOREIGN KEY(DepartmentID) references DEPT(DepartmentID);

-- select distinct departmentid from EMPL;
-- select count(departmentid) from empl where departmentid IS NOT NULL;
-- select count(*) from empl where;

-- INSERT INTO COMPANYDB.COMPANY_SCHEMA.DEPT(DepartmentName)
-- VALUES('Marketing');
-- UPDATE COMPANYDB.COMPANY_SCHEMA.DEPT
-- SET DepartmentID = 5
-- WHERE DepartmentName = 'Marketing';

DELETE FROM COMPANYDB.COMPANY_SCHEMA.DEPT
USING COMPANYDB.COMPANY_SCHEMA.EMPL
WHERE DEPT.DepartmentID = EMPL.DepartmentID
AND EMPL.DepartmentID = 5
AND DEPT.DepartmentName = 'Marketing';

-- *****OBSERVATION ==> It will not delete the all related employees of "Marketing" Department because CASCADING DELETE is not enforced here, thus not working.*****

-- Deleting All Records with a Condition
-- Delete All Records Based on a Common Attribute:
-- Delete all records from the Employees table where the JobTitle is 'Consultant'.
DELETE FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
WHERE JobTitles = 'Consultant';

-- Delete Records with NULL Values:
-- Delete all employees from the Employees table where the Salary is NULL.

DELETE FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
WHERE Salaries IS NULL;

-- Conditional DELETE Using Aggregate Functions:
-- Delete employees from the Employees table who earn less than the average salary of all employees.

DELETE FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES
WHERE Salaries < (SELECT AVG(Salaries) FROM COMPANYDB.COMPANY_SCHEMA.EMPLOYEES);
