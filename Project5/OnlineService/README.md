# NSDS_ApacheKafka
This repo holds the code for the Apache Kafka project of the 2023 edition of the Networked Software for Distributes Systems course.

The system will consist of four microservices: Users Service, Courses Service, Projects Service, and Registration Service.

#### Users Service:
Service accessed by **students**. Each student can:
1. Enroll into courses (Kafka Publisher -> topic: ```courses-Status:enrolled```)
2. See available projects (Kafka Consumer -> topic: ```projects-Status:posted```)
3. Submit solutions to projects (Kafka Publisher -> topic: ```projects-Status:ungraded```)
4. Check status of solutions (Kafka Consumer -> topic: ```projects-Status:graded```)
5. Check status (Kafka Consumer -> topic: ```courses-Status:passed```)
*Database*: contains personal data of registered students and professors (-> two ```private static Map<String, String>```)

Kafka Publisher Topics:
- courses (events triggered when a new student registers to a course)
- projects (events triggered when a student submits a solution to a project)

Kafka Consumer Topics:
- projects (events triggered when a student checks which projects have been posted by professor)
- courses (events triggered when a student checks status of course)
- projects (events triggered when a student checks status of solution to a specific project)

#### Courses Service:
Service accessed by **admin**. Each admin can:
1. Listens to new grades on projects (Kafka Consumer -> topic: ```projects-Status:graded```)
2. Publish whenever a course has been passed (Kafka Producer -> topic: ```course-Status:passed```)
*Database*: stores all pairs of project-grade

Kafka Topics:
- projects (events triggered when an admin fetches graded projects)
- courses (events triggered when an admin publishes a new completed course)

#### Projects Service:
Service accessed by **professor**. Each professor can:
1. Post new projects (Kafka Producer -> topic: ```projects-Status:posted```)
2. Look at new solutions posted by students (Kafka Consumer -> topic: ```projects-Status:ungraded```)
3. Grade solutions posted by students (Kafka Producer -> topic: ```projects-Status:graded```)
*Database*: nothing?

Kafka Topics:
- projects (events triggered when a professor posts a new project)
- projects (events triggered when a student submits a solution to a project)
- projects (events triggered when a project is graded)

#### Registration Service:
Standalone service (not accessed by anyone), Handles registration of grades for completed courses.
Determines if a course is completed for a student based on delivered projects and grades.
1. Listens to new completed courses (Kafka Consumer -> topic: ```course-Status:passed```)
*Database*: all completed course, for each student

Kafka Topics:
- course (events triggered when a new completed course is fetched)