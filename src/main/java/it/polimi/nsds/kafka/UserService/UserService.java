package it.polimi.nsds.kafka.UserService;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

public class UserService {
    private static Map<String, String> db_users;

    // kafka producer
    private KafkaProducer<String, String> userProducer = null;

    public UserService(Map<String, String> db_users) {
        this.db_users = db_users;
        final Properties propsProd = new Properties();
        propsProd.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsProd.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsProd.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        userProducer = new KafkaProducer<>(propsProd);

    }

    public String newUser(String user){
        String[] par = user.split(" ");
        String username = par[0];
        String password = par[1];
        String role = par[2];

        if (db_users.containsKey(username))
            return "Username already exists";

        db_users.put(username, password + " " + role);
        final ProducerRecord<String, String> newUser = new ProducerRecord<>("users", username, password + " " + role);
        userProducer.send(newUser);
        return "User " + username + " registered";
    }

    public String authenticateUser(String user) {
        String[] par = user.split(" ");
        String username = par[0];
        String password = par[1];
    
        if (db_users.containsKey(username)) {
            String storedCredentials = db_users.get(username);
            String[] storedPar = storedCredentials.split(" ");
            String storedPassword = storedPar[0];
            String role = storedPar[1];
    
            if (password.equals(storedPassword)) {
                return "LOGIN_SUCCESS";
            } else {
                return "Incorrect password";
            }
        } else {
            return "User not found";
        }
    }
}
