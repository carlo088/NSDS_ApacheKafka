# NSDS_ApacheKafka
This repo holds the code for the Apache Kafka project of the 2023 edition of the Networked Software for Distributes Systems course
The system will consist of four microservices: Users Service, Courses Service, Projects Service, and Registration Service.

#### Users Service:
Manages personal data of registered students and professors.
Handles user registration, including student enrollment and professor registration.

Kafka Topics:
- user-registered (events triggered when a new user is registered)
- student-enrolled (events triggered when a student enrolls in a course)

#### Courses Service:
Manages courses, which consist of several projects.
Handles course creation and removal by admins.

Kafka Topics:
- course-created (events triggered when a new course is created)
- course-removed (events triggered when a course is removed)

#### Projects Service:
Manages submission and grading of individual projects.
Professors post projects, and students submit solutions.

Kafka Topics:
- project-posted (events triggered when a professor posts a new project)
- project-submitted (events triggered when a student submits a solution)
- project-graded (events triggered when a project is graded)

#### Registration Service:
Handles registration of grades for completed courses.
Determines if a course is completed for a student based on delivered projects and grades.

Kafka Topics:
- grade-registered (events triggered when a grade is registered)
- course-completed (events triggered when a course is completed for a student)