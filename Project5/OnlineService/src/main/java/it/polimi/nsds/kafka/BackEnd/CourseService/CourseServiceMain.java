package it.polimi.nsds.kafka.BackEnd.CourseService;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Utils.ConfigUtils;
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

public class CourseServiceMain {
    // define a pool of threads to manage multiple client connections
    private static final ExecutorService executor = Executors.newFixedThreadPool(128);

    // define a consumer to recover data
    private static KafkaConsumer<String, String> recoverConsumer;

    public static void main(String[] args) throws IOException {
        // set the server socket
        ServerSocket serverSocket = new ServerSocket(ConfigUtils.courseServicePort);

        // initialize Kafka consumer used for recovering
        recoverConsumer = setRecoverConsumer();

        // recover data from Kafka records
        Map<String, String> db_courses = recoverCourses();
        Map<String, String> db_projects = recoverProjects();

        System.out.println("CourseService listening on port: " + ConfigUtils.courseServicePort);
        while(true){
            try {
                // accept a socket and run a thread for that client connection
                Socket socket = serverSocket.accept();
                CourseService courseService = new CourseService(socket, db_courses, db_projects);
                executor.submit(courseService);
                System.out.println("New connection established");
            } catch (IOException e){
                System.err.println("Connection error!");
            }
        }
    }

    /**
     * recovers projects state of the dbs from Kafka records (Note that recovering a topic can last 10 seconds if no record is found)
     * @return recovered db
     */
    private static Map<String, String> recoverProjects(){
        System.out.println("Recovering projects...");
        Map<String, String> db_recovered = new HashMap<>();

        recoverConsumer.subscribe(Collections.singletonList("projects"));
        final ConsumerRecords<String, String> records = recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

        for(final ConsumerRecord<String, String> record : records){
            System.out.println(record.key() + "=" + record.value());
            db_recovered.put(record.key(), record.value());
        }

        recoverConsumer.unsubscribe();
        return db_recovered;
    }

    /**
     * recovers courses state of the dbs from Kafka records (Note that recovering a topic can last 10 seconds if no record is found)
     * @return recovered db
     */
    private static Map<String, String> recoverCourses(){
        System.out.println("Recovering courses...");
        Map<String, String> db_recovered = new HashMap<>();
        Gson gson = new Gson();

        recoverConsumer.subscribe(Collections.singletonList("courses"));
        final ConsumerRecords<String, String> records = recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

        for(final ConsumerRecord<String, String> record : records){
            System.out.println(record.key() + "=" + record.value());
            Course course = gson.fromJson(record.value(), Course.class);
            if(!course.isOld()) {
                db_recovered.put(record.key(), record.value());
            } else {
                db_recovered.remove(record.key());
            }
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
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.kafkaBrokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "courseGroup");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(consumerProps);
    }
}
