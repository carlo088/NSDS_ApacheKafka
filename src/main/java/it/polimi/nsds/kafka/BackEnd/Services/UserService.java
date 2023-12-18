package it.polimi.nsds.kafka.BackEnd.Services;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class UserService {
    // users and courses stored in private data structures
    private final Map<String, String> db_users;
    private final Map<String, String> db_courses;

    // kafka producer
    private final KafkaProducer<String, String> userProducer;

    public UserService(Map<String, String> db_users, Map<String, String> db_courses) {
        this.db_users = db_users;
        this.db_courses = db_courses;

        userProducer = setUserProducer();

        // start a thread for the consumer
        UserConsumer consumer = new UserConsumer(db_courses);
        consumer.start();
    }

    /**
     * adds a new user
     * @param userJson user json
     * @return message for the client
     */
    public String newUser(String userJson){
        // get a User class from a Json file
        Gson gson = new Gson();
        User user = gson.fromJson(userJson, User.class);

        if (db_users.containsKey(user.getUsername()))
            return "Username already exists";

        // publish the record
        db_users.put(user.getUsername(), userJson);
        final ProducerRecord<String, String> newUser = new ProducerRecord<>("users", user.getUsername(), userJson);
        userProducer.send(newUser);
        return "User " + user.getUsername() + " registered";
    }

    /**
     * checks the credentials of the user with the ones on db
     * @param userJson user json
     * @return message for the client
     */
    public String authenticateUser(String userJson) {
        // get a User class from a Json file
        Gson gson = new Gson();
        User user = gson.fromJson(userJson, User.class);
    
        if (db_users.containsKey(user.getUsername())) {
            // get stored user
            String storedCredentials = db_users.get(user.getUsername());
            User storedUser = gson.fromJson(storedCredentials, User.class);
    
            if (user.getPassword().equals(storedUser.getPassword()) && storedUser.getRole().equals("STUDENT")) {
                return "STUDENT_SUCCESS";
            }else if(user.getPassword().equals(storedUser.getPassword()) && storedUser.getRole().equals("PROFESSOR")){
                return "PROFESSOR_SUCCESS";
            }
            else {
                return "Incorrect password";
            }
        } else {
            return "User not found";
        }
    }

    /**
     * shows all the courses for a user
     * @param username username
     * @return message for the client
     */
    public String showUserCourses(String username){
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
                response += (courseJson + " ");
            }
            return response;
        } else {
            return "User not found";
        }
    }

    /**
     * adds a course for a user
     * @param username username
     * @param courseID course id
     * @return message for the client
     */
    public synchronized String enrollCourse(String username, String courseID) {
        if (!db_users.containsKey(username))
            return "User not found";

        if (!db_courses.containsKey(courseID))
            return "Course not found";

        // Load user from db_users
        Gson gson = new Gson();
        User user = gson.fromJson(db_users.get(username), User.class);

        // if user isn't already enrolled, add the course
        if (user.getCourseIDs().contains(courseID))
            return "User is already enrolled in this course";

        user.addCourseID(courseID);

        // update user in db_users and Kafka
        String userJson = gson.toJson(user);
        db_users.put(username, userJson);

        final ProducerRecord<String, String> newUser = new ProducerRecord<>("users", user.getUsername(), userJson);
        userProducer.send(newUser);
        return "Enrolled in course";
    }

    /**
     * Kafka settings for User Producer
     * @return User Producer
     */
    private static KafkaProducer<String, String> setUserProducer(){
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }
}
