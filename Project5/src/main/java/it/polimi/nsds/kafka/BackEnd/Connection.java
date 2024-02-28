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

    public Connection(Socket socket, UserService userService, CourseService courseService,
                      ProjectService projectService, RegistrationService registrationService){
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
            send(new String[]{"Connection established!\n"});

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
    public void send(String[] message){
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
        String[] request = (String[]) in.readObject();
        String requestType = request[0];

        switch(requestType){
            case "REGISTER" :
                String[] response = userService.newUser(request[1]);
                send(response);
                break;
            case "LOGIN":
                response = userService.authenticateUser(request[1]);
                send(response);
                break;
            case "SHOW_USER_COURSES":
                response = userService.showUserCourses(request[1]);
                send(response);
                break;
            case "ENROLL":
                response = userService.enrollCourse(request[1], request[2]);
                send(response);
                break;
            case "POST":
                response = courseService.newProject(request[1]);
                send(response);
                break;
            case "ADD_COURSE":
                response = courseService.newCourse(request[1]);
                send(response);
                break;
            case "SHOW_ALL_COURSES":
                response = courseService.showAllCourses();
                send(response);
                break;
            case "REMOVE_COURSE":
                response = courseService.removeCourse(request[1]);
                send(response);
                break;
            case "SHOW_COURSE_PROJECTS":
                response = courseService.showCourseProjects(request[1]);
                send((response));
                break;
            case "SUBMIT_NEW":
                response = projectService.submitNewSolution(request[1]);
                send(response);
                break;
            case "SHOW_USER_SUBMISSIONS":
                response = projectService.checkSubmissionStatus(request[1]);
                send(response);
                break;
            case "SHOW_PROJECT_SUBMISSIONS":
                response = projectService.showNewSubmissions(request[1]);
                send(response);
                break;
            case "GRADE_SUBMISSION":
                response = projectService.updateSubmissionGrade(request[1], Integer.parseInt(request[2]));
                send(response);
                break;
            case "SHOW_USER_REGISTRATIONS":
                response = registrationService.showUserRegistrations(request[1]);
                send(response);
                break;
            default:
                send(new String[]{""});
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
