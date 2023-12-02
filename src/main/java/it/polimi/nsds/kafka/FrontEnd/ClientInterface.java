package it.polimi.nsds.kafka.FrontEnd;

import com.google.gson.Gson;
import com.sun.source.tree.ArrayAccessTree;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Beans.User;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static java.lang.Integer.parseInt;

public class ClientInterface {
    // object streams for socket connection
    private static ObjectInputStream in;
    private static ObjectOutputStream out;

    // scanner for stdin input
    private static Scanner input;

    // user session
    private static String usernameSession = null;

    public static void main(String[] args) throws IOException{
        // if there are arguments use for ip and port of socket connection, otherwise set the default
        String ip = args.length > 0 ? args[1] : "127.0.0.1";
        int port = args.length > 1 ? parseInt(args[2]) : 7268;

        // crate the stdin scanner
        input = new Scanner(System.in);

        try {
            // establish socket connection
            Socket socket = new Socket(ip, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());

            // start showing the interface
            System.out.println(receive());
            homePage();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Connection closed from the server side");
        } finally {
            in.close();
            out.close();
        }
    }

    /**
     * send a request to back-end through the socket's output stream
     * @param request request to send
     */
    public static void send(String request){
        try{
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
    private static String receive() throws IOException, ClassNotFoundException {
        return (String) in.readObject();
    }

    private static void homePage() throws IOException, ClassNotFoundException {
        System.out.println("Welcome to Online Services for continuous evaluation\n");
        boolean exit = false;
        while(!exit){
            System.out.println("Please press one of the following commands:");
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
        System.out.println("ADMIN Page:\n");
        boolean exit = false;
        while(!exit){
            System.out.println("Please press one of the following commands:");
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
                    System.out.println("Not a valid command:\n");
                    break;
            }
        }
    }

    private static void studentPage(){
        System.out.println("STUDENT Page:\n");
        boolean exit = false;
        while(!exit){
            System.out.println("Please press one of the following commands:");
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
                    System.out.println("Not a valid command:\n");
                    break;
            }
        }
    }

    private static void professorPage() throws IOException, ClassNotFoundException{
        System.out.println("PROFESSOR Page:\n");
        boolean exit = false;
        while(!exit){
            System.out.println("Please press one of the following commands:");
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
                    System.out.println("Not a valid command:\n");
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

        send("REGISTER" + " " + userJson);
        System.out.println(receive());
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

        send("LOGIN" + " " + userJson);
        String response = receive();

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
        String professor = null;
    
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
                if(numberOfProjects > 0)
                    valid = true;
                else
                    System.out.println("Number of projects must be more than zero");
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a valid number for projects.");
            }
        }

        System.out.println("Insert the username of the professor:");
        valid = false;
        while (!valid) {
            professor = input.nextLine();
            if (professor.contains(" ") || professor.length() == 0) {
                System.out.println("Invalid username");
            } else {
                valid = true;
            }
        }

        // create a bean Course and parse it to Json
        Course course = new Course(null, name, cfu, numberOfProjects, professor);
        Gson gson = new Gson();
        String courseJson = gson.toJson(course);
    
        send("ADD_COURSE" + " " + courseJson);
        String response = receive();
        System.out.println(response);
    }


    private static void removeCourse(){

    }

    private static void enrollCourse() throws IOException, ClassNotFoundException {
        System.out.println("Available courses:\n");
        send("SHOW_COURSES");
        String availableCourses = receive();

        // Print available courses
        System.out.println("Courses:");
        System.out.println(availableCourses);
        
        System.out.println("Choose a course by entering its ID:");
        String selectedCourseIDString = input.nextLine();

        try {
            int selectedCourseID = Integer.parseInt(selectedCourseIDString);
            if (availableCourses.contains(String.valueOf(selectedCourseID))) {
                send("ADD_ENROLL"+ " " + usernameSession + " " + selectedCourseID);
                String enrollResponse = receive();
                // print CourseService method output
                System.out.println(enrollResponse);
            } else {
                System.out.println("Invalid course ID. Please try again.");
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please enter a valid integer for the course ID.");
        }

    }

    private static void submitSolution(){

    }

    private static void checkSubmission(){

    }

    private static void postProject()  throws IOException, ClassNotFoundException{
        System.out.println("Your courses:\n");
        send("SHOW_COURSES" + " " + usernameSession);
        String response = receive();
        List<String> courseIds = new ArrayList<>(Arrays.asList(response.split(" ")));

        if(courseIds.isEmpty()){
            System.out.println("There are no courses");
            return;
        }

        for (String courseId: courseIds) {
            System.out.println(courseId);
        }

        String courseID = null;
        String projectID = null;
        String desc = null;

        boolean valid = false;
        System.out.println("Insert the course ID");
        while(!valid){
            courseID = input.nextLine();

            if (!courseIds.contains(courseID)){
                System.out.println("Invalid course ID, try again");
            }
            else {
                valid = true;
            }
        }

        valid = false;
        System.out.println("Insert the project ID");
        while (!valid){
            projectID = input.nextLine();
            
            if (projectID.contains(" ") || projectID.length() == 0){
                System.out.println("Invalid project ID, try again");
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

        
        send("POST" + " " + courseID + " " + projectID + " " + desc);
        System.out.println(receive());
    }

    private static void gradeSolution(){

    }
}