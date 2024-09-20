
CREATE DATABASE IF NOT EXISTS SchoolDB;

CREATE TABLE SCHOOLDB.Students (
    StudentID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DateOfBirth DATE,
    EnrolmentDate DATE,
    Major VARCHAR(50)
);
SHOW CREATE TABLE schooldb.students;

-- LOAD DATA INFILE 'D:\DATA_related\AWA\Assignments_myWork\SQL_Assignments\data_folder\students.csv' 
-- INTO TABLE students
-- FIELDS TERMINATED BY ',' 
-- ENCLOSED BY '"' 
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS;

-- Enforce Referential Integrity:
-- Add a foreign key constraint to the Students table, linking the Major column to a hypothetical Majors table that contains all valid majors offered by the school.

CREATE TABLE SchoolDB.Majors (
	MajorID INT PRIMARY KEY, 
    Major VARCHAR(50)
);
INSERT INTO schooldb.majors(MajorID, Major) 
VALUES (1,'Engineering'),
		(2,'Mathematics'),
        (3,'Art'),
        (4,'Science'),
        (5,'Computer Science'),
        (6,'History');
ALTER TABLE schooldb.students
DROP PRIMARY KEY;
ALTER TABLE schooldb.majors ADD INDEX(Major);
ALTER TABLE schooldb.students
ADD CONSTRAINT FK_Students_Major
FOREIGN KEY(Major) REFERENCES majors(Major) 
ON UPDATE CASCADE
ON DELETE CASCADE;
---------------------------------------------------------------------------------------------------------------------------------------------
-- Ensure Valid Date Values:
-- Add a check constraint to the Students table to ensure that the EnrollmentDate is not earlier than 2000-01-01.

ALTER TABLE students
ADD CONSTRAINT CC_EnrolmentDate
CHECK (students.EnrolmentDate >= str_to_date('2000-01-01','%Y-%m-%d'));

-- Composite Keys and Indexes
-- Composite Primary Key:
-- Create a composite primary key on the combination of FirstName and LastName in the Students table (assuming StudentID is no longer the primary key).

-- Finding if any duplicate entry exists for the FirstName LastName columns in students table
SELECT FirstName, LastName, COUNT(*)
FROM students
GROUP BY FirstName, LastName
HAVING COUNT(*) > 1;
-- creating a temporary table to store only single copy of each student from students table. 
-- It is done by grouping rows from students table by FirstName & LastName and then filtering out one student with lowest studentid for duplicate name entries.
CREATE TEMPORARY TABLE temp_students AS
SELECT MIN(StudentID) AS stu_id FROM students
GROUP BY FirstName,LastName;
-- From original students table, deleting those student records that are not available in temporary table.
DELETE FROM students
WHERE StudentID NOT IN (
	SELECT stu_id FROM temp_students
);
-- Now, we don't need the temopary table, so dropping it
DROP TEMPORARY TABLE temp_students;
-- Creating composite primary key, on combination of FirstName and LastName in students table.
ALTER TABLE students
ADD PRIMARY KEY(FirstName, LastName);

-- Rename Columns:
-- Rename the Major column in the Students table to Course to better reflect the data it stores.
ALTER TABLE students
DROP CONSTRAINT FK_Students_Major;

ALTER TABLE students
RENAME COLUMN Major TO Course;

-- Change the default value for the EnrollmentDate in the Students table to the current date.
ALTER TABLE students
MODIFY COLUMN EnrolmentDate DATETIME;
ALTER TABLE students
MODIFY COLUMN EnrolmentDate DATETIME DEFAULT CURRENT_TIMESTAMP;

-- For all students who enrolled before 2020-01-01, change their Course to 'Alumni'.
UPDATE students
SET Course = 'Alumni'
WHERE EnrolmentDate < '2020-01-01';

-- Add and Drop Indexes:
-- Add an index on the EnrollmentDate column in the Students table to speed up queries.
ALTER TABLE students
ADD INDEX(EnrolmentDate);

-- Add a column Graduated (BOOLEAN) to the Students table, then update this column to TRUE for all students whose Course is 'Alumni'.
ALTER TABLE students
ADD COLUMN Graduated BOOLEAN;
UPDATE students
SET Graduated = True
WHERE Course = 'Alumni';

-- Remove the student from the Students table whose StudentID is 3.
DELETE FROM students WHERE StudentID = 3;

-- Remove all students from the Students table who enrolled before 2019-01-01.
DELETE FROM students
WHERE EnrolmentDate < '2019-01-01';
-- Remove all students from the Students table whose Course is 'History' and who enrolled before 2021-01-01.
DELETE FROM students
WHERE Course = 'History' AND EnrolmentDate < '2021-01-01';

-- Remove all students from the Students table if there is no record of their major in a hypothetical Majors table.
ALTER TABLE majors
ADD INDEX(Major);
-- Insert one row to majors table which is Alumni, 
-- to fix error code 1452: 'Can't add or update a child row: 
-- "A foreign key constraint fails" --> indicating a mismatch of total count of course and major columns of two tables respectively.
INSERT INTO majors (MajorID, Major) VALUES (7, 'Alumni');

ALTER TABLE students
ADD CONSTRAINT FK_Course
FOREIGN KEY(Course) REFERENCES majors(Major) 
ON UPDATE CASCADE
ON DELETE CASCADE;

DELETE FROM students
WHERE Course NOT IN (SELECT Major FROM Majors);
-- DELETE with Cascading and Foreign Keys
-- Cascade DELETE Operations:
-- Remove a course from the Courses table (hypothetical) and ensure all students enrolled in that course are also removed.
ALTER TABLE courses
ADD INDEX FK_crs (Course);
ALTER TABLE students
ADD CONSTRAINT FK_crs
FOREIGN KEY (Course) REFERENCES courses(Course) ON DELETE CASCADE;
DELETE FROM courses WHERE Course = 'Art';
SELECT Course FROM students WHERE Course = 'Art';

-- Delete Using a Restriction:
-- Attempt to delete a student from the Students table if they are referenced in another table, such as a Grades table (hypothetical).

-- Before performing deletion from students table, different conditions arose, which I realized from facing some errors.
-- Thus, following steps needed to perform --> 
-- 1. like creating grades table and inserting some values from students table upon filtering condition, 
-- 2. add an index key for StudentID column in students table, for basically indexing the foreign key constraint FK_StudentID,
-- 3. set default value on GradePoints column and Graduated column of grades and students table respectively,
-- 4. updating GradePoints column values using built in functions rand() and round().
CREATE TABLE Grades(
    StudentID INT NOT NULL PRIMARY KEY,
    StudentFirstName VARCHAR(100) NOT NULL,
    StudentLastName VARCHAR(100) NOT NULL,
    Course VARCHAR(50) NOT NULL,
    GradePoints DECIMAL(7,2) NOT NULL
);
INSERT INTO `schooldb`.`grades`
(`StudentID`,
`StudentFirstName`,
`StudentLastName`,
`Course`) SELECT StudentID,FirstName,LastName,Course
FROM students
WHERE students.Graduated IS TRUE;
ALTER TABLE students ADD INDEX StID (StudentID);
ALTER TABLE students MODIFY COLUMN Graduated BOOLEAN DEFAULT FALSE;
ALTER TABLE grades MODIFY COLUMN GradePoints DECIMAL(7,2) DEFAULT 00.00;
UPDATE grades SET GradePoints = round(rand() * 10,1);

ALTER TABLE grades
ADD CONSTRAINT FK_StudentID
FOREIGN KEY(StudentID) REFERENCES students(StudentID);
-- 
DELETE FROM students WHERE StudentID = 101;

-- Deleting All Records with a Condition
-- Delete All Records Based on a Common Attribute:
-- Remove all records from the Students table where the Course is 'Art'.

DELETE FROM students
Where Course = 'Art';

-- Delete Records with NULL Values:
-- Remove all students from the Students table where the EnrollmentDate is NULL.

DELETE FROM students
WHERE EnrolmentDate IS NULL;

-- Conditional DELETE Using Aggregate Functions:
-- Remove students from the Students table who enrolled after the latest enrolment date in the table.

CREATE TEMPORARY TABLE latest_enrolment
AS
(SELECT MAX(EnrolmentDate) max_enrol_dt FROM students);
select * from latest_enrolment;
-- Now will compare with enrolment date value of the created temporary table, and delete those student records
DELETE FROM students WHERE EnrolmentDate > (SELECT max_enrol_dt FROM latest_enrolment);