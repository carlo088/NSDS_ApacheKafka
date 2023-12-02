package it.polimi.nsds.kafka.BackEnd;

import it.polimi.nsds.kafka.BackEnd.Services.CourseService;
import it.polimi.nsds.kafka.BackEnd.Services.ProjectService;
import it.polimi.nsds.kafka.BackEnd.Services.RegistrationService;
import it.polimi.nsds.kafka.BackEnd.Services.UserService;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Connection implements Runnable{
    // socket and streams
    private final Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    // services
    private final UserService userService;
    private final CourseService courseService;
    private final ProjectService projectService;
    private final RegistrationService registrationService;

    private boolean isActive = true;

    public Connection(Socket socket, UserService userService, CourseService courseService, ProjectService projectService, RegistrationService registrationService){
        this.socket = socket;
        this.userService = userService;
        this.courseService = courseService;
        this.projectService = projectService;
        this.registrationService = registrationService;
    }

    /**
     * starts the main process of the connection (receiving messages from the client)
     */
    @Override
    public void run() {
        try {
            // set socket streams
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            send("Connection established!\n");

            // connection is always waiting for requests from clients
            while(isActive){
                receive();
            }
        } catch(IOException e){
            System.err.println("Connection closed from client side");
        } catch (ClassNotFoundException e){
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

    /**
     * sends a response message to the client through the socket's output stream
     * @param message response message
     */
    public void send(String message){
        try {
            if (!isActive) {
                return;
            }
            out.writeObject(message);
            out.flush();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * receives a request message from the client through the socket's input stream (BLOCKING FUNCTION)
     * @throws IOException if there are IO problems
     * @throws ClassNotFoundException if there are problems with readObject() method
     */
    private void receive() throws IOException, ClassNotFoundException {
        String request = (String) in.readObject();
        String[] values = request.split(" ");
        String requestType = values[0];

        switch(requestType){
            case "REGISTER" :
                String response = userService.newUser(values[1]);
                send(response);
                break;
            case "LOGIN":
                response = userService.authenticateUser(values[1]);
                send(response);
                break;
            case "SHOW_COURSES":
                response = userService.showCourses(values[1]);
                send(response);
                break;
            case "POST":
                String buf = "";
                for (int i = 3; i < values.length; i++)
                    buf = buf + values[i];
                response = projectService.newProject(values[1] + " " + values[2] + " " + buf);
                send(response);
                break;
            case "ADD_COURSE":
                String addCourseResponse = courseService.newCourse(values[1]);
                send(addCourseResponse);
                break;
            case "ADD_ENROLL":
                String enrollResponse = courseService.enrollCourse(values[1], values[2]);
                send(enrollResponse);
                break;
            default:
                send("");
                break;
        }

    }

    /**
     * closes the connection
     */
    public synchronized void closeConnection(){
        try{
            socket.close();
        }catch (IOException e){
            System.err.println(e.getMessage());
        }
        System.out.println("Connection closed");
        isActive = false;
    }

}
