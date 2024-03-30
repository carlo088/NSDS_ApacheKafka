package it.polimi.nsds.kafka.BackEnd.OldServices;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Integer.parseInt;

public class OnlineServices {
    // define a pool of threads to manage multiple client connections
    private static final ExecutorService executor = Executors.newFixedThreadPool(128);

    // define a consumer to recover data from services
    private static KafkaConsumer<String, String> recoverConsumer;

    public static void main(String[] args) throws IOException{
        // if there are arguments use for the port of socket connection, otherwise set the default
        int port = args.length > 0 ? parseInt(args[0]) : 7269;

        // set the server socket
        ServerSocket serverSocket = new ServerSocket(port);

        // initialize Kafka consumer used for recovering
        recoverConsumer = setRecoverConsumer();

        // recover data from Kafka records
        Map<String, String> db_users = recover("users");
        Map<String, String> db_courses = recover("courses");
        Map<String, String> db_projects = recover("projects");
        Map<String, String> db_submissions = recover("submissions");
        Map<String, String> db_registrations = recover("registrations");

        // initialize all services
        OldUserService userService = new OldUserService(db_users, db_courses);
        OldCourseService courseService = new OldCourseService(db_courses, db_projects);
        OldProjectService projectService = new OldProjectService(db_submissions);
        OldRegistrationService registrationService = new OldRegistrationService(db_registrations, db_courses, db_submissions);

        System.out.println("OnlineServices listening on port: " + port);
        while(true){
            try {
                // accept a socket and run a thread for that client connection
                Socket socket = serverSocket.accept();
                Connection connection = new Connection(socket, userService, courseService, projectService, registrationService);
                executor.submit(connection);
                System.out.println("New connection established");
            } catch (IOException e){
                System.err.println("Connection error!");
            }
        }
    }

    /**
     * recovers state of the dbs from Kafka records (Note that recovering a topic can last 1 minute if no record is found)
     * @param topic topic to recover
     * @return recovered db
     */
    private static Map<String, String> recover(String topic){
        System.out.println("Recovering " + topic + "...");
        Map<String, String> db_recovered = new HashMap<>();

        recoverConsumer.subscribe(Collections.singletonList(topic));
        final ConsumerRecords<String, String> records = recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

        for(final ConsumerRecord<String, String> record : records){
            db_recovered.put(record.key(), record.value());
        }

        recoverConsumer.unsubscribe();
        return db_recovered;
    }

    /**
     * Kafka settings for Recover Consumer
     * @return Recover Consumer
     */
    private static KafkaConsumer<String, String> setRecoverConsumer(){
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "recoverGroup");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(consumerProps);
    }

}
