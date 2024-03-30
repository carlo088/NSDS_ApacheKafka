package it.polimi.nsds.kafka.BackEnd.UserService;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Beans.User;
import it.polimi.nsds.kafka.Utils.ConfigUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class UserService implements Runnable{
    // socket and streams
    private final Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    private boolean isActive = true;

    // users and courses stored in private data structures
    private final Map<String, String> db_users;
    private final Map<String, String> db_courses;

    // kafka producer
    private final KafkaProducer<String, String> userProducer;

    public UserService(Socket socket, Map<String, String> db_users, Map<String, String> db_courses){
        this.socket = socket;
        this.db_users = db_users;
        this.db_courses = db_courses;

        userProducer = setUserProducer();

        // start a thread for the consumer
        UserConsumer consumer = new UserConsumer(db_courses);
        consumer.start();
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
            send(new String[]{"Connection to User Service established!"});

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

        switch(requestType){
            case "REGISTER" :
                String[] response = newUser(request[1]);
                send(response);
                break;
            case "LOGIN":
                response = authenticateUser(request[1]);
                send(response);
                break;
            case "SHOW_USER_COURSES":
                response = showUserCourses(request[1]);
                send(response);
                break;
            case "ENROLL":
                response = enrollCourse(request[1], request[2]);
                send(response);
                break;
            default:
                send(new String[]{""});
                break;
        }
    }

    /**
     * adds a new user
     * @param userJson user json
     * @return message for the client
     */
    public String[] newUser(String userJson){
        // get a User class from a Json file
        Gson gson = new Gson();
        User user = gson.fromJson(userJson, User.class);

        if (db_users.containsKey(user.getUsername()))
            return new String[]{"Username already exists"};

        // publish the record
        db_users.put(user.getUsername(), userJson);
        final ProducerRecord<String, String> newUser = new ProducerRecord<>("users", user.getUsername(), userJson);
        userProducer.send(newUser);
        return new String[]{"User " + user.getUsername() + " registered"};
    }

    /**
     * checks the credentials of the user with the ones on db
     * @param userJson user json
     * @return message for the client
     */
    public String[] authenticateUser(String userJson) {
        // get a User class from a Json file
        Gson gson = new Gson();
        User user = gson.fromJson(userJson, User.class);

        if (db_users.containsKey(user.getUsername())) {
            // get stored user
            String storedCredentials = db_users.get(user.getUsername());
            User storedUser = gson.fromJson(storedCredentials, User.class);

            if (user.getPassword().equals(storedUser.getPassword()) && storedUser.getRole().equals("STUDENT")) {
                return new String[]{"STUDENT_SUCCESS"};
            }else if(user.getPassword().equals(storedUser.getPassword()) && storedUser.getRole().equals("PROFESSOR")){
                return new String[]{"PROFESSOR_SUCCESS"};
            }
            else {
                return new String[]{"Incorrect password"};
            }
        } else {
            return new String[]{"User not found"};
        }
    }

    /**
     * shows all the courses for a user
     * @param username username
     * @return message for the client
     */
    public String[] showUserCourses(String username){
        if (db_users.containsKey(username)) {
            // get logged user
            String userJson = db_users.get(username);
            Gson gson = new Gson();
            User user = gson.fromJson(userJson, User.class);

            // get all course IDs
            List<String> courseIDs = user.getCourseIDs();
            String response = "";
            for (String courseID: courseIDs) {
                String courseJson = db_courses.get(courseID);
                Course course = gson.fromJson(courseJson, Course.class);
                if(!course.isOld())
                    response += (courseJson + " ");
            }
            return response.split(" ");
        } else {
            return new String[]{"User not found"};
        }
    }

    /**
     * adds a course for a user
     * @param username username
     * @param courseID course id
     * @return message for the client
     */
    public synchronized String[] enrollCourse(String username, String courseID) {
        if (!db_users.containsKey(username))
            return new String[]{"User not found"};

        if (!db_courses.containsKey(courseID))
            return new String[]{"Course not found"};

        // Load user from db_users
        Gson gson = new Gson();
        User user = gson.fromJson(db_users.get(username), User.class);

        // if user isn't already enrolled, add the course
        if (user.getCourseIDs().contains(courseID))
            return new String[]{"User is already enrolled in this course"};

        user.addCourseID(courseID);

        // update user in db_users and Kafka
        String userJson = gson.toJson(user);
        db_users.put(username, userJson);

        final ProducerRecord<String, String> newUser = new ProducerRecord<>("users", user.getUsername(), userJson);
        userProducer.send(newUser);
        return new String[]{"Enrolled in course"};
    }

    /**
     * Kafka settings for User Producer
     * @return User Producer
     */
    private static KafkaProducer<String, String> setUserProducer(){
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.kafkaBrokers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }
}
