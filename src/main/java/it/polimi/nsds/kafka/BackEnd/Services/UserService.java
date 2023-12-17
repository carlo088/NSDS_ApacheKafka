package it.polimi.nsds.kafka.BackEnd.Services;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.User;
import it.polimi.nsds.kafka.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Map;

public class UserService {
    // users and courses stored in private data structures
    private final Map<String, String> db_users;
    private final Map<String, String> db_courses;

    // kafka producer
    private final KafkaProducer<String, String> userProducer;

    public UserService(Map<String, String> db_users, Map<String, String> db_courses) {
        this.db_users = db_users;
        this.db_courses = db_courses;

        userProducer = Utils.setProducer();

        // start a thread for the consumer
        UserConsumer consumer = new UserConsumer(db_courses);
        consumer.start();
    }

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

    public String showProfessors(){
        Gson gson = new Gson();
        String response = "";
        for (String userJson: db_users.values()) {
            User user = gson.fromJson(userJson, User.class);
            if(user.getRole().equals("PROFESSOR"))
                response += (user.getUsername() + " ");
        }
        return response;
    }

    // TODO: rendere sincronizzata con il metodo removeCourse
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

    // TODO: rendere sincronizzata con il metodo removeCourse
    public String enrollCourse(String username, String courseID) {
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
}
