package it.polimi.nsds.kafka.front_end;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Scanner;

public class ClientInterface {

    private static KafkaProducer<String, String> producer = null;
    private static KafkaConsumer<String, String> consumer = null;

    private static Scanner in;

    public static void main(String[] args){
        in = new Scanner(System.in);

        final Properties propsCons = new Properties();
        propsCons.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsCons.put(ConsumerConfig.GROUP_ID_CONFIG, "client");
        propsCons.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        propsCons.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "15000");
        propsCons.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        propsCons.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsCons.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(propsCons);

        final Properties propsProd = new Properties();
        propsProd.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsProd.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsProd.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(propsProd);

        homePage();
    }

    private static void homePage(){
        System.out.println("Welcome to Online Services for continuous evaluation\n");
        boolean exit = false;
        while(!exit){
            System.out.println("Please press one of the following commands:");
            System.out.println("REGISTER\nLOGIN\nADMIN\nQUIT\n");
            String command = in.nextLine().toUpperCase();
            switch(command) {
                case "REGISTER":
                    register();
                    break;
                case "LOGIN":
                    login();
                    break;
                case "ADMIN":
                    adminPage();
                    break;
                case "QUIT":
                    exit = true;
                    break;
                default:
                    System.out.println("Not a valid command");
                    break;
            }
        }
    }

    private static void adminPage(){
        System.out.println("ADMIN Page:\n\n");
        System.out.println("Please press one of the following commands:\n");
        System.out.println("ADD\nREMOVE\nHOME\n");
        boolean exit = false;
        while(!exit){
            String command = in.nextLine().toUpperCase();
            switch(command) {
                case "ADD":
                    addCourse();
                    break;
                case "REMOVE":
                    removeCourse();
                    break;
                case "HOME":
                    homePage();
                    exit = true;
                    break;
                default:
                    System.out.println("Not a valid command:\n");
                    break;
            }
        }
    }

    //gli username sono univoci per studenti e professori indipendentemente dal ruolo
    private static void register(){
        String username = null;
        String password = null;
        String role = null;
        System.out.println("Insert a username:");
        boolean valid = false;
        while(!valid) {
            username = in.nextLine();

            final ConsumerRecords<String, String> users = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
            boolean alreadyExists = false;
            for (final ConsumerRecord<String, String> user : users) {
                if (username.equals(user.key())) {
                    alreadyExists = true;
                    break;
                }
            }

            if (username.contains(" ") || username.length() == 0 || alreadyExists){
                System.out.println("Invalid username");
            }
            else {
                valid = true;
            }
        }

        System.out.println("Insert a password:");
        valid = false;
        while(!valid) {
            password = in.nextLine();
            if (password.contains(" ") || password.length() == 0){
                System.out.println("Invalid password");
            }
            else {
                valid = true;
            }
        }

        System.out.println("Are you a STUDENT or PROFESSOR?");
        valid = false;
        while(!valid) {
            role = in.nextLine().toUpperCase();
            if (role.equals("STUDENT") || role.equals("PROFESSOR"))
                valid = true;
            else
                System.out.println("Not a valid role");
        }

        final ProducerRecord<String, String> newUser = new ProducerRecord<>("users", username, role + " " + password);
        producer.send(newUser);
        System.out.println("User " + username + " registered");
    }

    private static void login(){

    }

    private static void addCourse(){

    }

    private static void removeCourse(){

    }
}