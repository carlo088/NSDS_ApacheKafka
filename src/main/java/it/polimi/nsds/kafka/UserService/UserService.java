package it.polimi.nsds.kafka.UserService;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

public class UserService {
    // users stored in a private data structure
    private final Map<String, String> db_users;

    // kafka producer
    private final KafkaProducer<String, String> userProducer;

    public UserService(Map<String, String> db_users) {
        this.db_users = db_users;
        userProducer = Utils.setProducer();
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
}
