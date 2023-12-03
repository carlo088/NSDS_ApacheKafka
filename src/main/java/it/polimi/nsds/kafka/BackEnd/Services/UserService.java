package it.polimi.nsds.kafka.BackEnd.Services;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.User;
import it.polimi.nsds.kafka.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Map;

public class UserService {
    // users stored in a private data structure
    private final Map<String, String> db_users;

    // kafka producer
    private final KafkaProducer<String, String> userProducer;

    public UserService(Map<String, String> db_users) {
        this.db_users = db_users;
        userProducer = Utils.setProducer();
        UserConsumer consumer = new UserConsumer(db_users);
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

    public String showUserCourses(String username){
        if (db_users.containsKey(username)) {
            // get logged user
            String userJson = db_users.get(username);
            Gson gson = new Gson();
            User user = gson.fromJson(userJson, User.class);

            // get all courses
            List<String> courseIds = user.getCourseIds();
            String response = "";
            for (String course: courseIds) {
                response += (course + " ");
            }
            return response;
        } else {
            return "User not found";
        }
    }

    public String enrollCourse(String username, String courseId) {
        if (!db_users.containsKey(username))
            return "User not found";

        //FIXME: il controllo risulterebbe molto più complesso nel caso il corso venga eliminato durante una enroll
        /*
        if (!db_courses.containsKey(courseId))
            return "Course not found";
         */

        // Load user from db_users
        Gson gson = new Gson();
        User user = gson.fromJson(db_users.get(username), User.class);

        // if user isn't already enrolled, add the course
        if (user.getCourseIds().contains(courseId))
            return "User is already enrolled in course " + courseId;

        user.addCourse(courseId);

        // update the CourseService db_users value associated with the key username in the db_users map
        db_users.put(username, gson.toJson(user));
        String userJson = gson.toJson(user);
        final ProducerRecord<String, String> newUser = new ProducerRecord<>("users", user.getUsername(), userJson);
        userProducer.send(newUser);

        // TODO Send record of new enrollment such that Registration service can pull it?

        return "Enrolled in course " + courseId;
    }
}
