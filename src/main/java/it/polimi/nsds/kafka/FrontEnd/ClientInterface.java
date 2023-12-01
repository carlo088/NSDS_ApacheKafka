package it.polimi.nsds.kafka.FrontEnd;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;

import static java.lang.Integer.parseInt;

public class ClientInterface {
    // object streams for socket connection
    private static ObjectInputStream in;
    private static ObjectOutputStream out;

    // scanner for stdin input
    private static Scanner input;

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
            receive();
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
     * receives and prints a response from back-end through the socket's input stream
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

    private static void adminPage(){
        System.out.println("ADMIN Page:\n");
        System.out.println("Please press one of the following commands:");
        System.out.println("ADD\nREMOVE\nHOME\n");
        boolean exit = false;
        while(!exit){
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
        System.out.println("Please press one of the following commands:");
        System.out.println("ENROLL\nSUBMIT\nCHECK\nHOME\n");
        boolean exit = false;
        while(!exit){
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
        System.out.println("Please press one of the following commands:");
        System.out.println("POST\nGRADE\nHOME\n");
        boolean exit = false;
        while(!exit){
            String command = input.nextLine().toUpperCase();
            switch(command) {
                case "POST":
                    postProject();
                    break;
                case "GRADE":
                    gradeSolution();
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

    //gli username sono univoci per studenti e professori indipendentemente dal ruolo
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

        send("REGISTER" + " " + username + " " + password + " " + role);
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

        send("LOGIN" + " " + username + " " + password);
        String response = receive();

        if (response.equals("STUDENT_SUCCESS")) {
            System.out.println("Login successful!");
            studentPage();
        }else if(response.equals("PROFESSOR_SUCCESS")){
            System.out.println("Login successful!");
            professorPage();
        }
         else {
            System.out.println("Login failed. Please check your username and password.");
        }
    }

    private static void addCourse(){

    }

    private static void removeCourse(){

    }

    private static void enrollCourse(){

    }

    private static void submitSolution(){

    }

    private static void checkSubmission(){

    }

    private static void postProject()  throws IOException, ClassNotFoundException{

        String courseID = null;
        String projectID = null;
        String desc = null;

        boolean valid = false;
        System.out.println("Insert the course ID");
        while(!valid){
            courseID = input.nextLine();

            if (courseID.contains(" ") || courseID.length() == 0){
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
       
        professorPage();
    }

    private static void gradeSolution(){

    }
}