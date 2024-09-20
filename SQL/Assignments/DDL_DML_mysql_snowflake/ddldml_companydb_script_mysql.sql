CREATE TABLE COMPANYDB.Employees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DateOfBirth DATE,
    JobTitle VARCHAR(50),
    Salary DECIMAL(10,2),
    DepartmentID INT,
    Email VARCHAR(50)
);

select * from COMPANYDB.Employees;

CREATE TABLE COMPANYDB.Departments (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(100)
);

select * from COMPANYDB.Departments;

ALTER TABLE COMPANYDB.EMPLOYEES
ADD COLUMN HireDate DATE;

ALTER TABLE COMPANYDB.DEPARTMENTS 
MODIFY COLUMN DEPARTMENTNAME VARCHAR(150);

-- 2.Data Insertion, Updating, and Deletion (DML)
-- 1.1 Insert Data:
-- 1.2 Insert five records into the Employees table.

-- LOAD DATA INFILE 'D:\DATA_related\AWA\Assignments_myWork\SQL_Assignments\data_folder\employees.csv'
-- INTO TABLE `companydb`.`employee`
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS;

select count(*) from companydb.employees;
INSERT INTO COMPANYDB.EMPLOYEES(
    EMPLOYEEID, 
    FIRSTNAME, 
    LASTNAME, 
    DATEOFBIRTH, 
    JOBTITLE, 
    SALARY, 
    DEPARTMENTID, 
    EMAIL, 
    HIREDATE) VALUES (1001,'Sayantan','Naha',str_to_date('1989-11-16','%Y-%m-%d'),'Intern',15000.76,3,'sayantannaha77@gmail.com',str_to_date('2024-08-15','%Y-%m-%d')),
                    (1002,'Sandip','Saha',str_to_date('1988-11-16','%Y-%m-%d'),'Consultant',50000.75,3,'sandip.saha@gmail.com',str_to_date('2017-10-27','%Y-%m-%d')),
                    (1003,'Samit','Dutta',str_to_date('1992-07-07','%Y-%m-%d'),'Sales Representative',25000.45,2,'sumit.dutta@gmail.com',str_to_date('2024-01-25','%Y-%m-%d')),
                    (1004,'Subhajit','Ghosh',str_to_date('1990-12-09','%Y-%m-%d'),'HR Manager',48000.59,1,'subhajit.ghosh@gmail.com',str_to_date('2020-10-15','%Y-%m-%d')),
                    (1005,'Sayak','Dutta',str_to_date('1993-05-15','%Y-%m-%d'),'Intern',15000.76,4,'sayak.dutta@gmail.com',str_to_date('2024-07-16','%Y-%m-%d'));
                  
-- Insert three records into the Departments table.
select count(*) from departments;
INSERT INTO COMPANYDB.DEPARTMENTS(
    DEPARTMENTID, 
    DEPARTMENTNAME) VALUES (6,'Operations'),(7,'Supply Chain'),(8,'Finance');
    
-- 2 Update Data:
-- 2.1 Update the Salary of the employee with EmployeeID = 3 to 75000.
-- 2.2 Update the Position of all employees where JobTitle is Intern to Junior Developer.

UPDATE COMPANYDB.EMPLOYEES
SET Salary = 75000
WHERE EmployeeID = 3;

UPDATE COMPANYDB.EMPLOYEES
SET JobTitle = 'Junior Developer'
WHERE JobTitle = 'Intern';

-- 3. Delete Data:
-- 3.1 Delete the employee record with EmployeeID = 4.
-- 3.2 Delete all records from the Departments table where the DepartmentName is HR.

DELETE FROM COMPANYDB.EMPLOYEES
WHERE EmployeeID = 4;

DELETE FROM COMPANYDB.DEPARTMENTS
WHERE DepartmentName = 'HR';

-- 4. Complex Insert and Update:
-- 4.1 Insert a new employee, ensuring that their DepartmentID exists in the Departments table.
-- 4.2 Update all employees who have NULL in the HireDate column to the current date.
-- select * from employees;
INSERT INTO COMPANYDB.EMPLOYEES(
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
        str_to_date('1991-11-16','%Y-%m-%d'),
        'Intern',
        15003.76,
        4,
        'jayanta.sen77@gmail.com',
        str_to_date('2024-08-15','%Y-%m-%d')
    WHERE EXISTS (
        SELECT DEPARTMENTID FROM COMPANYDB.DEPARTMENTS
        );

UPDATE COMPANYDB.EMPLOYEES
SET HIREDATE = CURRENT_DATE()
WHERE HIREDATE IS NULL;

-- 3. Data Selection and Filtering (DML)
-- 1. Select Data:
-- 1.1 Select all columns from the Employees table.
-- 1.2 Select the FirstName, LastName, and Salary of all employees who have a salary greater than 60000.

SELECT * FROM COMPANYDB.EMPLOYEES;
SELECT FirstName, LastName, Salary FROM COMPANYDB.EMPLOYEES WHERE Salary > 60000;

-- 2. Filtering and Sorting:
-- 2.1 Select all employees from the Employees table who were hired after 2018-01-01.
-- 2.2 Select all employees from the Employees table and order them by LastName in ascending order.

SELECT * FROM COMPANYDB.EMPLOYEES WHERE HIREDATE > '2018-01-01';

SELECT * FROM COMPANYDB.EMPLOYEES
ORDER BY LASTNAME ASC;

-- 3. Aggregate Functions:
-- 3.1 Count the total number of employees in the Employees table.
-- 3.2 Calculate the average Salary of all employees.

SELECT COUNT(*) FROM COMPANYDB.EMPLOYEES;
SELECT AVG(SALARY) FROM COMPANYDB.EMPLOYEES;

-- 4. Primary Key and Foreign Key Constraints
-- 4.1 Enforce Uniqueness:
-- 4.1.1 Ensure that the EmployeeID in the Employees table is unique and cannot be NULL.
-- 4.1.2 Ensure that each department in the Departments table has a unique DepartmentID that is also not NULL.

ALTER TABLE companydb.employees
DROP PRIMARY KEY;
-- ALTER TABLE companydb.employees
-- DROP CONSTRAINT PK_EmployeeID;
ALTER TABLE companydb.employees
ADD PRIMARY KEY(EmployeeID);
-- ALTER TABLE COMPANYDB.EMPLOYEES
-- ADD CONSTRAINT PK_EmployeeID PRIMARY KEY (EmployeeID);
SHOW CREATE TABLE companydb.employees;
ALTER TABLE companydb.departments
DROP PRIMARY KEY;
ALTER TABLE companydb.departments
ADD PRIMARY KEY(DepartmentID);
SHOW CREATE TABLE companydb.departments;

-- 4.2 Establish Relationships:
-- 4.2.1 Modify the Employees table to add a DepartmentID column (if not already present) and create a foreign key relationship between the Employees table and the Departments table on DepartmentID.
-- 4.2.2 Ensure that the DepartmentID in the Employees table cannot have a value that does not exist in the Departments table.
ALTER TABLE Employees
MODIFY COLUMN DepartmentID INT NOT NULL;

ALTER TABLE departments
ADD PRIMARY KEY(departmentid);

SET FOREIGN_KEY_CHECKS=0;

ALTER TABLE Employees
ADD FOREIGN KEY (DepartmentID)
REFERENCES Departments(DepartmentID);

-- TO ENSURE REFERENCIAL INTEGRITY, CONSISTANCY BETWEEN TWO TABLES
ALTER TABLE Employees
ADD FOREIGN KEY (DepartmentID)
REFERENCES Departments(DepartmentID)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- 4.3. Cascade on Delete:
-- 4.3.1 Modify the foreign key in the Employees table so that if a department is deleted from the Departments table, all employees associated with that department are also deleted.

-- ALTER TABLE Employees
-- ADD FOREIGN KEY (DepartmentID)
-- REFERENCES Departments(DepartmentID)
-- ON DELETE CASCADE
-- ON UPDATE CASCADE;

-- 4.4 Enforce Referential Integrity:
-- Unique and Not Null Constraints
-- 1. Ensure Unique Values:
-- Add a unique constraint on the FirstName and LastName combination in the Employees table, ensuring that no two employees can have the same first and last name combination.
-- Ensure that the DepartmentName in the Departments table is unique.

-- SELECT FirstName, LastName, COUNT(*)
-- FROM Employees
-- GROUP BY firstname, lastname
-- HAVING COUNT(*) > 1;

-- DELETE e1
-- FROM Employees e1
-- JOIN Employees e2
-- ON e1.firstname = e2.firstname
-- WHERE e1.lastname > e2.lastname;

ALTER TABLE Employees
ADD CONSTRAINT UC_Empl_Fullname UNIQUE (FirstName, LastName);
SHOW INDEX FROM Employees;
-- ALTER TABLE Employees DROP `employees`;
-- ALTER TABLE Employees DROP `departmentid`;

ALTER TABLE COMPANYDB.DEPARTMENTS
ADD CONSTRAINT UC_DeptName UNIQUE (DepartmentName);

-- 2. Prevent NULL Values:
-- Modify the Employees table to ensure that the FirstName, LastName, DateOfBirth, and Salary columns cannot contain NULL values.
-- In the Departments table, ensure that the DepartmentName cannot be NULL.

ALTER TABLE COMPANYDB.EMPLOYEES
MODIFY COLUMN FirstName VARCHAR(50) NOT NULL;

ALTER TABLE COMPANYDB.EMPLOYEES
MODIFY COLUMN LastName VARCHAR(50)
NOT NULL;        

ALTER TABLE COMPANYDB.EMPLOYEES
MODIFY COLUMN DateOfBirth DATE
NOT NULL;

ALTER TABLE COMPANYDB.EMPLOYEES
MODIFY COLUMN Salary DECIMAL(10,2)
NOT NULL;

ALTER TABLE COMPANYDB.DEPARTMENTS
MODIFY COLUMN DepartmentName VARCHAR(150)
NOT NULL;

-- Default and Check Constraints
-- Set Default Values:
-- 1. Add a default value of 'Unknown' to the Position column in the Employees table, so if no position is specified, it will default to 'Unknown'.
-- 2. Set a default value of '1000' for the Salary column in the Employees table.

ALTER TABLE COMPANYDB.EMPLOYEES
ADD COLUMN JobTitle VARCHAR(50) DEFAULT 'Unknown';

ALTER TABLE COMPANYDB.EMPLOYEES
MODIFY COLUMN `Salary`decimal(10,2) DEFAULT 1000;

-- Enforce Valid Data Ranges with CHECK:
-- 1. Add a check constraint to the Employees table that ensures the Salary is greater than 0.
-- 2. Add a check constraint to the Departments table to ensure that the DepartmentName is at least 3 characters long.

ALTER TABLE COMPANYDB.EMPLOYEES
ADD CHECK(Salary > 0);

ALTER TABLE COMPANYDB.DEPARTMENTS
ADD CONSTRAINT CC_DeptName
CHECK (LENGTH(DepartmentName) >= 3);

-- Ensure Valid Date Values:
-- 1. Add a check constraint to the Employees table to ensure that the HireDate is not in the future.
-- 2. Add a check constraint to the Students table to ensure that the EnrollmentDate is not earlier than 2000-01-01.
ALTER TABLE COMPANYDB.EMPLOYEES
MODIFY COLUMN HireDate DATE;
ALTER TABLE COMPANYDB.EMPLOYEES
ADD CONSTRAINT CC_HireDate
CHECK (HireDate <= DATE_FORMAT(curdate(), '%Y-%m-%d'));

-- 2. In the Employees table, create a composite primary key on the combination of EmployeeID and HireDate (if HireDate is unique for each employee).

-- SELECT employeeid, hiredate, COUNT(*)
-- FROM Employees
-- GROUP BY employeeid, hiredate
-- HAVING COUNT(*) > 1;

-- DELETE e1
-- FROM employees e1
-- JOIN employees e2
-- ON e1.EmployeeID = e2.EmployeeID
-- WHERE e1.HireDate > e2.HireDate;
ALTER TABLE employees
DROP PRIMARY KEY;
ALTER TABLE COMPANYDB.EMPLOYEES
ADD CONSTRAINT pk_EmpID_HireDate
PRIMARY KEY (EmployeeID, HireDate);

-- Composite Unique Key:
-- 1. Add a composite unique key on the combination of FirstName and LastName in the Departments table to ensure no two departments can have the same head's first and last name combination.

ALTER TABLE COMPANYDB.DEPARTMENTS
ADD COLUMN HeadFirstName VARCHAR(50),
ADD COLUMN HeadLastName VARCHAR(50);

ALTER TABLE COMPANYDB.DEPARTMENTS
ADD CONSTRAINT CUK_Head_FullName
UNIQUE (HeadFirstName, HeadLastName);

-- Practice Questions on UPDATE, ALTER, and MODIFY Commands
-- 1. Altering and Modifying Table Structures
-- 1. Modify Data Types:
-- Change the Salary column in the Employees table to a larger precision, for example, DECIMAL(12,2) to allow for higher salaries.
-- Modify the DateOfBirth column in the Employees table from DATE to DATETIME to include time of birth.

ALTER TABLE COMPANYDB.EMPLOYEES
MODIFY COLUMN Salary DECIMAL(12,2);

ALTER TABLE employees
MODIFY COLUMN DateOfBirth datetime;

-- 2. Add New Columns:
-- Add a new column Email (VARCHAR(100)) to the Employees table to store employee email addresses.
-- Add a new column DepartmentHead (BOOLEAN) to the Departments table to indicate if a department has a head.

ALTER TABLE COMPANYDB.EMPLOYEES
ADD COLUMN Email VARCHAR(100);

ALTER TABLE COMPANYDB.DEPARTMENTS
ADD COLUMN DepartmentHead BOOLEAN;

-- 4. Remove Columns:
-- Drop the HireDate column from the Employees table as it is no longer needed.

ALTER TABLE COMPANYDB.EMPLOYEES
DROP COLUMN HireDate;

-- Remove the DepartmentHead column from the Departments table.

ALTER TABLE COMPANYDB.DEPARTMENTS
DROP COLUMN departmentHead;

-- 5. Change Default Values:
-- Modify the default value for the Position column in the Employees table to 'Employee' instead of 'Unknown'.

ALTER TABLE COMPANYDB.EMPLOYEES
MODIFY column JobTitles varchar(50) DEFAULT 'Employee';

-- 2. Updating Data
-- 1.Basic Updates:
-- Update the Salary of all employees with the Position of 'Junior Developer' to 70000.

UPDATE COMPANYDB.EMPLOYEES
SET Salary = 70000
WHERE JobTitles = 'Junior Developer';

-- Change the DepartmentName of the department with DepartmentID = 2 to 'Research & Development'.

UPDATE COMPANYDB.DEPARTMENTS
SET DepartmentName = 'Research & Development'
Where DepartmentID = 2;

-- 2. Conditional Updates:
-- Update the JobTitle of employees who were hired before 2015-01-01 to 'Senior Developer'.

UPDATE COMPANYDB.EMPLOYEES
SET JobTitle = 'Senior Developer'
WHERE Hiredate < '2015-01-01';

-- 3. Bulk Updates:
-- Increase the Salary of all employees by 10%.
UPDATE employees SET salary = salary + salary*0.1;
-- Set the DepartmentID of all employees currently in the HR department to NULL (assuming the department is being dissolved).
UPDATE employees SET departmentid = NULL
WHERE departmentid = (
    SELECT departmentid FROM departments 
    WHERE departmentname = 'HR'
);
-- 4. Update Using Joins:
-- Update the Salary of employees in the Sales department to 80000 using a join between Employees and Departments on DepartmentID.
-- Set the DepartmentHead to TRUE for the department that has an employee named 'John Doe'.

UPDATE Employees
INNER JOIN Departments ON Employees.DepartmentID = Departments.DepartmentID
SET Employees.Salary = 80000
WHERE Departments.DepartmentName = 'Sales';

ALTER TABLE departments ADD COLUMN DepartmentHead BOOLEAN;
UPDATE departments
INNER JOIN employees ON departments.DepartmentID = employees.DepartmentID
SET departmenthead = TRUE
WHERE employees.FirstName = 'John' AND employees.LastName = 'Doe';

-- 3.Advanced Table Modifications
-- Reorganize Table:
-- Change the order of columns in the Employees table to have LastName appear before FirstName.

ALTER TABLE employees
MODIFY COLUMN FirstName VARCHAR(50) AFTER LastName;

-- Reorder the Departments table so that DepartmentID is the last column.

ALTER TABLE departments
MODIFY COLUMN DepartmentID INT AFTER DepartmentHead;

-- Drop and Add Constraints:
-- Drop the foreign key constraint on DepartmentID in the Employees table, and then add it back with ON DELETE CASCADE.
ALTER TABLE COMPANYDB.EMPLOYEES
DROP FOREIGN KEY DepartmentID;

ALTER TABLE employees
ADD CONSTRAINT fk_DepartmentID 
FOREIGN KEY (DepartmentID)
references DEPARTMENTS (DepartmentID) ON DELETE CASCADE;

-- Drop the unique constraint on the combination of FirstName and LastName in the Employees table.

ALTER TABLE COMPANYDB.EMPLOYEES
DROP CONSTRAINT UC_Empl_Fullname;

-- Add and Drop Indexes:
-- Drop the composite index on FirstName and LastName in the Employees table.
ALTER TABLE employees
DROP INDEX `UC_Empl_Fullname`;

-- Combined Operations
-- Update and Alter Combined:
-- First, update all employee Salaries to NULL, then modify the Salary column to set a NOT NULL constraint with a default value of 50000.

-- here employees.salary column is already set to NULL as default value, below 2 lines of codes are commented out..

-- UPDATE employees
-- SET Salary = NULL; 

ALTER TABLE employees
MODIFY COLUMN Salary DECIMAL(12,2) 
NOT NULL 
DEFAULT 50000;

-- Add a new column PhoneNumber to the Employees table and immediately populate it with a default value for all existing rows.
ALTER TABLE employees
ADD COLUMN PhoneNumber DECIMAL(12,0) DEFAULT 91;

-- Modify and Update Combined:
-- Modify the JobTitle column to accept a maximum of 100 characters, then update all employees with the title 'Intern' to have the title 'Temporary Employee'.
ALTER TABLE employees
MODIFY JobTitle VARCHAR(100);

UPDATE employees
SET Jobtitle = 'Temporary Employee'
WHERE JobTitle = 'Intern';

-- Practice Questions on DELETE Commands with Conditions
-- Basic DELETE Operations
-- Delete a Specific Record:
-- Delete the employee from the Employees table where the EmployeeID is 5.

DELETE FROM COMPANYDB.EMPLOYEES WHERE EMPLOYEEID = 5;

-- Delete Multiple Records Based on a Condition:
-- Delete all employees from the Employees table who have a Salary less than 50000.

DELETE FROM COMPANYDB.EMPLOYEES WHERE Salary < 50000;

-- DELETE with Complex Conditions
-- Delete Using AND/OR Conditions:
-- Delete all employees from the Employees table who are either in the HR department or have a JobTitle of 'Intern'.

-- Because, in a previous query, departmentname HR was deleted, below I inserted one record 'HRD' into departmentname column in departments table,
-- with respective DepartmentID value 1 & DepartmentName is 'HRD', not 'HR' as in ques asked, because of one Check Constraint 'CC_DeptName' says DepartmentName's length
-- must be >= 3 
INSERT INTO 
COMPANYDB.DEPARTMENTS(DepartmentName, DepartmentID) 
VALUES ('HRD',1);

-- Now, Using AND/OR conditions..
DELETE empl FROM employees AS empl
INNER JOIN departments AS dept
WHERE empl.DepartmentID = dept.DepartmentID
AND dept.DepartmentName = 'HRD' OR empl.JobTitle = 'Intern';

-- Delete Using EXISTS Clause:
-- Delete all employees from the Employees table where the department they belong to no longer exists in the Departments table.

select distinct departmentid from employees;
DELETE FROM employees empl
WHERE NOT EXISTS (SELECT 1 FROM departments dept
                  WHERE empl.DepartmentID = dept.DepartmentID);


-- DELETE with Cascading and Foreign Keys
-- Cascade DELETE Operations:
-- Delete a department from the Departments table and ensure all related employees are also deleted (requires foreign key with ON DELETE CASCADE).

-- Note: ON DELETE CASCADE is previously set when adding a foreign key constraint to employees table
DELETE dept FROM departments AS dept
INNER JOIN employees AS empl
WHERE dept.DepartmentName = 'Research & Development';
-- Just Checking if any employees from Research & Development department exists in employees table
select EmployeeID from employees AS e
INNER JOIN departments AS d
ON e.DepartmentID = d.DepartmentID
WHERE d.DepartmentName = 'Research & Development';
-- Delete Using a Restriction:
-- Attempt to delete a department from the Departments table where there are still employees assigned to that department, and observe what happens (if foreign keys are not set to cascade).

-- Experimentation, If foreign keys are not set to cascade :: Before deleting a department from departments table we need to modify the foreign key setup.
ALTER TABLE employees
ADD CONSTRAINT fk_EmployeeID
FOREIGN KEY(DepartmentID) REFERENCES departments(DepartmentID);

DELETE dept FROM departments AS dept
INNER JOIN employees AS empl
ON dept.DepartmentID = empl.DepartmentID
WHERE dept.DepartmentName = 'Sales';
-- *****OBSERVATION ==> It returns an Error Code: 1451. "Cannot delete or update a parent row: a foreign key constraint fails". It indicates a foreign key constraint violation. 
--  It will not delete the all related employees of "Sales" Department because CASCADING DELETE is not set when the foreign key constraint was modified in previous step, thus not working.
-- There are associated 'Sales' department records that exists in the employees table when we are trying to delete that department 'Sales' from departments table, and 
-- the foreign key constraint prevent to do so.*****

-- Deleting All Records with a Condition
-- Delete All Records Based on a Common Attribute:
-- Delete all records from the Employees table where the JobTitle is 'Consultant'.
DELETE FROM employees
WHERE JobTitle = 'Consultant';

-- Delete Records with NULL Values:
-- Delete all employees from the Employees table where the Salary is NULL.

DELETE FROM employees
WHERE Salary IS NULL;

-- Conditional DELETE Using Aggregate Functions:
-- Delete employees from the Employees table who earn less than the average salary of all employees.

DELETE e FROM employees e
JOIN (SELECT AVG(Salary) AS avg_sal FROM employees) AS avg_sal_table
WHERE e.Salary < avg_sal_table.avg_sal;