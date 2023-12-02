package it.polimi.nsds.kafka.UserService;

import it.polimi.nsds.kafka.ProjectService.ProjectService;
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
    private final ProjectService projectService;

    private boolean isActive = true;

    public Connection(Socket socket, UserService userService, ProjectService projectService){
        this.socket = socket;
        this.userService = userService;
        this.projectService = projectService;
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
            case "POST":
                String buf = "";
                for (int i = 3; i < values.length; i++)
                    buf = buf + values[i];
                response = projectService.newProject(values[1] + " " + values[2] + " " + buf);
                send(response);
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
