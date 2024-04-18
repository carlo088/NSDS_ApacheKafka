package it.polimi.nsds.kafka.FrontEnd;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.*;
import it.polimi.nsds.kafka.Utils.ConfigUtils;
import it.polimi.nsds.kafka.Utils.Service;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;

public class ClientInterface {
    // object streams for socket connections
    private static final Map<Service, ObjectInputStream> inputStreams = new HashMap<>();
    private static final Map<Service, ObjectOutputStream> outputStreams = new HashMap<>();

    // scanner for stdin input
    private static Scanner input;

    // user session
    private static String usernameSession = null;

    public static void main(String[] args) throws IOException{
        // create the stdin scanner
        input = new Scanner(System.in);

        try {
            // establish connection with user service
            Socket socket = new Socket(ConfigUtils.userServiceIP, ConfigUtils.userServicePort);
            inputStreams.put(Service.USER, new ObjectInputStream(socket.getInputStream()));
            outputStreams.put(Service.USER, new ObjectOutputStream(socket.getOutputStream()));
            System.out.println(receive(Service.USER)[0]);

            // establish connection with course service
            socket = new Socket(ConfigUtils.courseServiceIP, ConfigUtils.courseServicePort);
            inputStreams.put(Service.COURSE, new ObjectInputStream(socket.getInputStream()));
            outputStreams.put(Service.COURSE, new ObjectOutputStream(socket.getOutputStream()));
            System.out.println(receive(Service.COURSE)[0]);

            // establish connection with project service
            socket = new Socket(ConfigUtils.projectServiceIP, ConfigUtils.projectServicePort);
            inputStreams.put(Service.PROJECT, new ObjectInputStream(socket.getInputStream()));
            outputStreams.put(Service.PROJECT, new ObjectOutputStream(socket.getOutputStream()));
            System.out.println(receive(Service.PROJECT)[0]);

            // establish connection with registration service
            socket = new Socket(ConfigUtils.registrationServiceIP, ConfigUtils.registrationServicePort);
            inputStreams.put(Service.REGISTRATION, new ObjectInputStream(socket.getInputStream()));
            outputStreams.put(Service.REGISTRATION, new ObjectOutputStream(socket.getOutputStream()));
            System.out.println(receive(Service.REGISTRATION)[0]);

            // start showing the interface
            homePage();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Connection closed from the server side");
        } finally {
            for (ObjectInputStream in : inputStreams.values()) {
                in.close();
            }
            for (ObjectOutputStream out : outputStreams.values()) {
                out.close();
            }
        }
    }

    /**
     * send a request to back-end through the socket's output stream
     * @param request request to send
     */
    public static void send(String[] request, Service service){
        try{
            ObjectOutputStream out = outputStreams.get(service);
            out.writeObject(request);
            out.reset();
            out.flush();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * receives and returns a response from back-end through the socket's input stream
     * @return string received
     * @throws IOException if there are IO problems
     * @throws ClassNotFoundException if there are problems with readObject() method
     */
    private static String[] receive(Service service) throws IOException, ClassNotFoundException {
        ObjectInputStream in = inputStreams.get(service);
        return (String[]) in.readObject();
    }

    private static void homePage() throws IOException, ClassNotFoundException {
        System.out.println("Welcome to Online Services for continuous evaluation");
        boolean exit = false;
        while(!exit){
            System.out.println("\nPlease press one of the following commands:");
            System.out.println("REGISTER\nLOGIN\nADMIN\nQUIT\n");
            String command = input.nextLine().toUpperCase();
            switch(command) {
                case "REGISTER":
                    register();
                    break;
                case "LOGIN":
                    login();
                    break;
                case "ADMIN":
                    adminPage();
                    break;
                case "QUIT":
                    exit = true;
                    break;
                default:
                    System.out.println("Not a valid command");
                    break;
            }
        }
    }

    private static void adminPage() throws IOException, ClassNotFoundException {
        System.out.println("\nADMIN Page:");
        boolean exit = false;
        while(!exit){
            System.out.println("\nPlease press one of the following commands:");
            System.out.println("ADD\nREMOVE\nHOME\n");
            String command = input.nextLine().toUpperCase();
            switch(command) {
                case "ADD":
                    addCourse();
                    break;
                case "REMOVE":
                    removeCourse();
                    break;
                case "HOME":
                    exit = true;
                    break;
                default:
                    System.out.println("Not a valid command:");
                    break;
            }
        }
    }

    private static void studentPage() throws IOException, ClassNotFoundException {
        System.out.println("\nSTUDENT Page:");
        showUserRegistrations();
        boolean exit = false;
        while(!exit){
            System.out.println("\nPlease press one of the following commands:");
            System.out.println("ENROLL\nSUBMIT\nCHECK\nHOME\n");
            String command = input.nextLine().toUpperCase();
            switch(command) {
                case "ENROLL":
                    enrollCourse();
                    break;
                case "SUBMIT":
                    submitSolution();
                    break;
                case "CHECK":
                    checkSubmission();
                    break;
                case "HOME":
                    usernameSession = null;
                    exit = true;
                    break;
                default:
                    System.out.println("Not a valid command:");
                    break;
            }
        }
    }

    private static void professorPage() throws IOException, ClassNotFoundException{
        System.out.println("\nPROFESSOR Page:");
        boolean exit = false;
        while(!exit){
            System.out.println("\nPlease press one of the following commands:");
            System.out.println("POST\nGRADE\nHOME\n");
            String command = input.nextLine().toUpperCase();
            switch(command) {
                case "POST":
                    postProject();
                    break;
                case "GRADE":
                    gradeSolution();
                    break;
                case "HOME":
                    usernameSession = null;
                    exit = true;
                    break;
                default:
                    System.out.println("Not a valid command:");
                    break;
            }
        }
    }

    private static void register() throws IOException, ClassNotFoundException {
        String username = null;
        String password = null;
        String role = null;
        System.out.println("Insert a username:");
        boolean valid = false;
        while(!valid) {
            username = input.nextLine();

            if (username.contains(" ") || username.length() == 0){
                System.out.println("Invalid username");
            }
            else {
                valid = true;
            }
        }

        System.out.println("Insert a password:");
        valid = false;
        while(!valid) {
            password = input.nextLine();
            if (password.contains(" ") || password.length() == 0){
                System.out.println("Invalid password");
            }
            else {
                valid = true;
            }
        }

        System.out.println("Are you a STUDENT or PROFESSOR?");
        valid = false;
        while(!valid) {
            role = input.nextLine().toUpperCase();
            if (role.equals("STUDENT") || role.equals("PROFESSOR"))
                valid = true;
            else
                System.out.println("Not a valid role");
        }

        // create a bean User and parse it to Json
        User user = new User(username, password, role, new ArrayList<>());
        Gson gson = new Gson();
        String userJson = gson.toJson(user);

        send(new String[]{"REGISTER", userJson}, Service.USER);
        System.out.println(receive(Service.USER)[0]);
    }

    private static void login() throws IOException, ClassNotFoundException{
        String username = null;
        String password = null;
        
        System.out.println("Insert your username:");
        boolean valid = false;
        while (!valid) {
            username = input.nextLine();
            if (username.contains(" ") || username.length() == 0) {
                System.out.println("Invalid username");
            } else {
                valid = true;
            }
        }

        System.out.println("Insert your password:");
        valid = false;
        while (!valid) {
            password = input.nextLine();
            if (password.contains(" ") || password.length() == 0) {
                System.out.println("Invalid password");
            } else {
                valid = true;
            }
        }

        // create a bean User and parse it to Json
        User user = new User(username, password, null, null);
        Gson gson = new Gson();
        String userJson = gson.toJson(user);

        send(new String[]{"LOGIN", userJson}, Service.USER);
        String response = receive(Service.USER)[0];

        if (response.equals("STUDENT_SUCCESS")) {
            System.out.println("Login successful!");
            usernameSession = username;
            studentPage();
        }else if(response.equals("PROFESSOR_SUCCESS")){
            System.out.println("Login successful!");
            usernameSession = username;
            professorPage();
        }
         else {
            System.out.println("Login failed. Please check your username and password.");
        }
    }

    private static void addCourse() throws IOException, ClassNotFoundException {
        String name = null;
        int cfu = 0;
        int numberOfProjects = 0;
    
        System.out.println("Insert the name of the course:");
        boolean valid = false;
        while (!valid) {
            name = input.nextLine();
            if (name.contains(" ") || name.length() == 0) {
                System.out.println("Invalid name");
            } else {
                valid = true;
            }
        }

        System.out.println("Insert number of cfu:");
        valid = false;
        while (!valid) {
            String cfuInput = input.nextLine();
            try {
                cfu = Integer.parseInt(cfuInput);
                if(cfu > 0)
                    valid = true;
                else
                    System.out.println("CFU must be more than zero");
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a valid number for projects.");
            }
        }
    
        System.out.println("Insert number of projects:");
        valid = false;
        while (!valid) {
            String numberOfProjectsInput = input.nextLine();
            try {
                numberOfProjects = Integer.parseInt(numberOfProjectsInput);
                if(numberOfProjects > 0 && numberOfProjects < 6)
                    valid = true;
                else
                    System.out.println("Number of projects must be more than zero and no more than five");
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a valid number for projects.");
            }
        }

        // create a bean Course and parse it to Json
        Course course = new Course(null, name, cfu, numberOfProjects, new ArrayList<>());
        Gson gson = new Gson();
        String courseJson = gson.toJson(course);
    
        send(new String[]{"ADD_COURSE", courseJson}, Service.COURSE);
        System.out.println(receive(Service.COURSE)[0]);
    }


    private static void removeCourse() throws IOException, ClassNotFoundException{
        send(new String[]{"SHOW_ALL_COURSES"}, Service.COURSE);
        String[] response = receive(Service.COURSE);
        List<String> courses = new ArrayList<>(Arrays.asList(response));

        if (courses.isEmpty() || courses.get(0).equals("")){
            System.out.println("There are no available courses");
            return;
        }

        System.out.println("Available courses:\n");
        List<String> courseIDs = new ArrayList<>();
        Gson gson = new Gson();
        for (String courseJson: courses) {
            Course course = gson.fromJson(courseJson, Course.class);
            System.out.println("ID = " + course.getId() + " | NAME = " + course.getName() + " | CFU = " + course.getCfu() +
                    " | #PROJECTS = " + course.getProjectNum() + " |");

            courseIDs.add(course.getId());
        }

        System.out.println("Choose a course by entering its ID:");
        String selectedCourseID = null;
        boolean valid = false;
        while(!valid){
            selectedCourseID = input.nextLine();
            if (!courseIDs.contains(selectedCourseID)) {
                System.out.println("Invalid course ID. Please try again.");
            } else {
                valid = true;
            }
        }

        send(new String[]{"REMOVE_COURSE", selectedCourseID}, Service.COURSE);
        System.out.println(receive(Service.COURSE)[0]);
    }

    private static void enrollCourse() throws IOException, ClassNotFoundException {
        send(new String[]{"SHOW_ALL_COURSES"}, Service.COURSE);
        String[] response = receive(Service.COURSE);
        List<String> courses = new ArrayList<>(Arrays.asList(response));

        if (courses.isEmpty() || courses.get(0).equals("")){
            System.out.println("There are no available courses");
            return;
        }

        System.out.println("Available courses:\n");
        List<String> availableIDs = new ArrayList<>();
        Gson gson = new Gson();
        for (String courseJson: courses) {
            Course course = gson.fromJson(courseJson, Course.class);
            System.out.println("ID = " + course.getId() + " | NAME = " + course.getName() + " | CFU = " + course.getCfu() +
                    " | #PROJECTS = " + course.getProjectNum()  + " |");

            availableIDs.add(course.getId());
        }
        
        System.out.println("Choose a course by entering its ID:");
        String selectedCourseID = null;
        boolean valid = false;
        while(!valid){
            selectedCourseID = input.nextLine();
            if (!availableIDs.contains(selectedCourseID)) {
                System.out.println("Invalid course ID. Please try again.");
            } else {
                valid = true;
            }
        }

        send(new String[]{"ENROLL", usernameSession, selectedCourseID}, Service.USER);
        String enrollResponse = receive(Service.USER)[0];
        System.out.println(enrollResponse);
    }

    private static void submitSolution() throws IOException, ClassNotFoundException {
        send(new String[]{"SHOW_USER_COURSES", usernameSession}, Service.USER);
        String[] response = receive(Service.USER);
        List<String> courses = new ArrayList<>(Arrays.asList(response));

        if(courses.isEmpty() || courses.get(0).equals("")){
            System.out.println("There are no courses");
            return;
        }

        // find registered courses
        List<String> registeredCourses = new ArrayList<>();
        Gson gson = new Gson();
        send(new String[]{"SHOW_USER_REGISTRATIONS", usernameSession}, Service.REGISTRATION);
        response = receive(Service.REGISTRATION);
        List<String> registrations = new ArrayList<>(Arrays.asList(response));
        if (!registrations.isEmpty() && !registrations.get(0).equals("")) {
            for (String registrationJson : registrations) {
                Registration registration = gson.fromJson(registrationJson, Registration.class);
                registeredCourses.add(registration.getCourse());
            }
        }

        System.out.println("Available projects:\n");
        List<String> availableIDs = new ArrayList<>();
        for (String courseJson: courses) {
            Course course = gson.fromJson(courseJson, Course.class);

            // don't show registered courses
            if (registeredCourses.contains(course.getId()))
                continue;

            System.out.println(course.getName());
            send(new String[]{"SHOW_COURSE_PROJECTS", course.getId()}, Service.COURSE);
            String[] resp = receive(Service.COURSE);
            List<String> projects = new ArrayList<>(Arrays.asList(resp));
            if(projects.isEmpty() || projects.get(0).equals("")){
                System.out.println("No projects available for this course");
                break;
            }
            else {
                for (String projectJson : projects) {
                    Project project = gson.fromJson(projectJson, Project.class);
                    System.out.println("ID = " + project.getId() + " | DESCRIPTION = " + project.getDescription());
                    availableIDs.add(project.getId());
                }
            }
            System.out.println();
        }

        if(availableIDs.isEmpty())
            return;

        System.out.println("Choose a project by entering its ID:");
        String selectedProjectID = null;
        boolean valid = false;
        while(!valid){
            selectedProjectID = input.nextLine();
            if (!availableIDs.contains(selectedProjectID)) {
                System.out.println("Invalid project ID. Please try again.");
            } else {
                valid = true;
            }
        }

        System.out.println("Submit solution for project " + selectedProjectID + ":");
        String projectSolution = null;
        valid = false;
        while(!valid){
            projectSolution = input.nextLine();
            if (projectSolution.equals("")) {
                System.out.println("Invalid solution. Please try again.");
            } else {
                valid = true;
            }
        }

        Submission submission = new Submission(null, selectedProjectID, usernameSession, projectSolution, -1);
        String submissionJson = gson.toJson(submission);

        send(new String[]{"SUBMIT_NEW", submissionJson}, Service.PROJECT);
        String projectSolutionResponse = receive(Service.PROJECT)[0];
        System.out.println(projectSolutionResponse);
    }

    private static void checkSubmission() throws IOException, ClassNotFoundException{
        send(new String[]{"SHOW_USER_SUBMISSIONS", usernameSession}, Service.PROJECT);
         String[] response = receive(Service.PROJECT);
         List<String> submissions = new ArrayList<>(Arrays.asList(response));

         if(submissions.isEmpty() || submissions.get(0).equals("")){
            System.out.println("There are no submissions");
            return;
        }

        System.out.println("Select one of the following submission by entering its ID:");
        Map<String, String> sentSubmissions = new HashMap<>();
        Gson gson = new Gson();
        for (String submissionJson: submissions) {
            Submission submission = gson.fromJson(submissionJson, Submission.class);
            System.out.println("ID = " + submission.getId() + " | PROJECT_ID = " + submission.getProjectId()
                    + " | SOLUTION = " + submission.getSolution()+ " |");

            sentSubmissions.put(submission.getId(), submissionJson);
        }
        
        String submissionID = null;
        boolean valid = false;
        System.out.println("Insert the submission ID");
        while(!valid){
            submissionID = input.nextLine();

            if (!sentSubmissions.containsKey(submissionID)){
                System.out.println("Invalid submission ID, try again");
            }
            else {
                valid = true;
            }
        }

        Submission sub = gson.fromJson(sentSubmissions.get(submissionID), Submission.class);
        int notGraded = -1;
        int grade = sub.getGrade();

        if (grade == notGraded){
            System.out.println("This submission has not been graded yet");
            return;
        }

        System.out.println("This submission has been graded, the grade is: " + grade);

    }

    private static void postProject()  throws IOException, ClassNotFoundException{
        send(new String[]{"SHOW_ALL_COURSES"}, Service.COURSE);
        String[] response = receive(Service.COURSE);
        List<String> courses = new ArrayList<>(Arrays.asList(response));

        if(courses.isEmpty() || courses.get(0).equals("")){
            System.out.println("There are no courses");
            return;
        }

        System.out.println("Available courses:\n");
        Map<String, String> availableCourses = new HashMap<>();
        Gson gson = new Gson();
        for (String courseJson: courses) {
            Course course = gson.fromJson(courseJson, Course.class);
            System.out.println("ID = " + course.getId() + " | NAME = " + course.getName() + " |");

            availableCourses.put(course.getId(), courseJson);
        }

        String courseID = null;
        String desc = null;

        boolean valid = false;
        System.out.println("Insert the course ID");
        while(!valid){
            courseID = input.nextLine();

            if (!availableCourses.containsKey(courseID)){
                System.out.println("Invalid course ID, try again");
            }
            else {
                valid = true;
            }
        }

        System.out.println("Insert the project description");

        valid = false;
        while(!valid){
            desc = input.nextLine();

            if (desc.equals("")){
                System.out.println("Invalid description, try again");
            }else{
                valid = true;
            }
        }

        Project project = new Project(null, desc, courseID);
        String projectJson = gson.toJson(project);

        send(new String[]{"POST", projectJson}, Service.COURSE);
        System.out.println(receive(Service.COURSE)[0]);
    }

    private static void gradeSolution() throws IOException, ClassNotFoundException{
        send(new String[]{"SHOW_ALL_COURSES"}, Service.COURSE);
        String[] response = receive(Service.COURSE);
        List<String> courses = new ArrayList<>(Arrays.asList(response));

        if (courses.isEmpty() || courses.get(0).equals("")){
            System.out.println("There are no available courses");
            return;
        }

        System.out.println("Available courses:\n");
        Map<String, String> availableCourses = new HashMap<>();
        Gson gson = new Gson();
        for (String courseJson: courses) {
            Course course = gson.fromJson(courseJson, Course.class);
            System.out.println("ID = " + course.getId() + " | NAME = " + course.getName() + " | CFU = " + course.getCfu() +
                    " | #PROJECTS = " + course.getProjectNum() + " |");

            availableCourses.put(course.getId(), courseJson);
        }

        String courseID = null;
        boolean valid = false;
        System.out.println("Insert the course ID");
        while(!valid){
            courseID = input.nextLine();

            if (!availableCourses.containsKey(courseID)){
                System.out.println("Invalid course ID, try again");
            }
            else {
                valid = true;
            }
        }

        send(new String[]{"SHOW_COURSE_PROJECTS", courseID}, Service.COURSE);
        String[] resp = receive(Service.COURSE);
        List<String> projects = new ArrayList<>(Arrays.asList(resp));
        List<String> projectIDs = new ArrayList<>();

        if(projects.isEmpty() || projects.get(0).equals("")){
                System.out.println("No projects available for this course");
                return;
            }
            else {
                for (String projectJson : projects) {
                    Project project = gson.fromJson(projectJson, Project.class);
                    System.out.println("ID = " + project.getId() + " | DESCRIPTION = " + project.getDescription());
                    projectIDs.add(project.getId());
                }
        }

        String projectID = null;
        valid = false;
        System.out.println("Insert the project ID");
        while(!valid){
            projectID = input.nextLine();

            if (!projectIDs.contains(projectID)){
                System.out.println("Invalid project ID, try again");
            }
            else {
                valid = true;
            }
        }

        send(new String[]{"SHOW_PROJECT_SUBMISSIONS", projectID}, Service.PROJECT);
        response = receive(Service.PROJECT);
        List<String> submissions = new ArrayList<>(Arrays.asList(response));
        List<String> submissionIDs = new ArrayList<>();

        if(submissions.isEmpty() || submissions.get(0).equals("")){
                System.out.println("There is no submission for this project");
                return;
            }
            else {
                for (String submissionJson : submissions) {
                    Submission submission = gson.fromJson(submissionJson, Submission.class);
                    System.out.println("ID = " + submission.getId() + " | STUDENT = " + submission.getStudentUsername()
                            + " | SOLUTION = " + submission.getSolution());
                    submissionIDs.add(submission.getId());
                }
        }


        String submissionID = null;
        valid = false;
        System.out.println("Insert the submission ID");
        while(!valid){
            submissionID = input.nextLine();

            if (!submissionIDs.contains(submissionID)){
                System.out.println("Invalid submission ID, try again");
            }
            else {
                valid = true;
            }
        }

        int grade = -1;
        Course selectedCourse = gson.fromJson(availableCourses.get(courseID), Course.class);
        int maxGrade = 33 / selectedCourse.getProjectNum();
        valid = false;
        System.out.println("Insert the grade for this submission");
        while(!valid){
            try {
                grade = Integer.parseInt(input.nextLine());
                if (grade > maxGrade || grade < 0){
                    System.out.println("Grade has to be positive and less than " + maxGrade + ", try again");
                }
                else {
                    valid = true;
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a valid number for grade.");
            }
        }

        send(new String[]{"GRADE_SUBMISSION", submissionID, String.valueOf(grade)}, Service.PROJECT);
        System.out.println(receive(Service.PROJECT)[0]);
    }

    private static void showUserRegistrations() throws IOException, ClassNotFoundException {
        send(new String[]{"SHOW_USER_REGISTRATIONS", usernameSession}, Service.REGISTRATION);
        String[] response = receive(Service.REGISTRATION);
        List<String> registrations = new ArrayList<>(Arrays.asList(response));

        if(registrations.isEmpty() || registrations.get(0).equals("")){
            return;
        }

        System.out.println("\nCompleted courses:\n");
        Gson gson = new Gson();
        for (String registrationJson: registrations) {
            Registration registration = gson.fromJson(registrationJson, Registration.class);
            System.out.println("COURSE = " + registration.getCourse() + " | GRADE = " + registration.getGrade() + " |");
        }
    }
}