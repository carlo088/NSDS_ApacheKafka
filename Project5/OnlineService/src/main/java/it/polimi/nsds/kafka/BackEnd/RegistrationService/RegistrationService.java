package it.polimi.nsds.kafka.BackEnd.RegistrationService;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Registration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

public class RegistrationService implements Runnable{
    // socket and streams
    private final Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    private boolean isActive = true;

    // users and courses stored in private data structures
    private final Map<String, String> db_registrations;

    public RegistrationService(Socket socket, Map<String, String> db_registrations, Map<String, String> db_courses, Map<String, String> db_submissions){
        this.socket = socket;
        this.db_registrations = db_registrations;

        // start a thread for the consumer
        RegistrationConsumer consumer = new RegistrationConsumer(db_courses);
        consumer.start();

        // start a thread for the producer
        RegistrationProducer producer = new RegistrationProducer(db_registrations, db_submissions, db_courses);
        producer.start();
    }

    /**
     * starts the main process of the service (receiving messages from the client)
     */
    @Override
    public void run() {
        try {
            // set socket streams
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            send(new String[]{"Connection to Registration Service established!\n"});

            // service is always waiting for requests from clients
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

    /**
     * receives a request message from the client through the socket's input stream (BLOCKING FUNCTION)
     * @throws IOException if there are IO problems
     * @throws ClassNotFoundException if there are problems with readObject() method
     */
    private void receive() throws IOException, ClassNotFoundException {
        String[] request = (String[]) in.readObject();
        String requestType = request[0];

        if (requestType.equals("SHOW_USER_REGISTRATIONS")) {
            String[] response = showUserRegistrations(request[1]);
            send(response);
        } else {
            send(new String[]{""});
        }
    }

    /**
     * shows user registrations
     * @param username username
     * @return message for the client
     */
    public String[] showUserRegistrations(String username){
        Gson gson = new Gson();
        String response = "";
        for (String registrationJson: db_registrations.values()) {
            Registration registration = gson.fromJson(registrationJson, Registration.class);
            if(registration.getUsername().equals(username))
                response += registrationJson + " ";
        }
        return response.split(" ");
    }
}
