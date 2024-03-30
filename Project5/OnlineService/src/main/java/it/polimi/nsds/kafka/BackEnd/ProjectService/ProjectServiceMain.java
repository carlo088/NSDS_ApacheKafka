package it.polimi.nsds.kafka.BackEnd.ProjectService;

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

public class ProjectServiceMain {
    // define a pool of threads to manage multiple client connections
    private static final ExecutorService executor = Executors.newFixedThreadPool(128);

    // define a consumer to recover data
    private static KafkaConsumer<String, String> recoverConsumer;

    public static void main(String[] args) throws IOException {
        // set the server socket
        ServerSocket serverSocket = new ServerSocket(ConfigUtils.projectServicePort);

        // initialize Kafka consumer used for recovering
        recoverConsumer = setRecoverConsumer();

        // recover data from Kafka records
        Map<String, String> db_submissions = recover();

        System.out.println("ProjectService listening on port: " + ConfigUtils.projectServicePort);
        while(true){
            try {
                // accept a socket and run a thread for that client connection
                Socket socket = serverSocket.accept();
                ProjectService projectService = new ProjectService(socket, db_submissions);
                executor.submit(projectService);
                System.out.println("New connection established");
            } catch (IOException e){
                System.err.println("Connection error!");
            }
        }
    }

    /**
     * recovers state of the dbs from Kafka records (Note that recovering a topic can last 10 seconds if no record is found)
     * @return recovered db
     */
    private static Map<String, String> recover(){
        System.out.println("Recovering submissions...");
        Map<String, String> db_recovered = new HashMap<>();

        recoverConsumer.subscribe(Collections.singletonList("submissions"));
        final ConsumerRecords<String, String> records = recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

        for(final ConsumerRecord<String, String> record : records){
            System.out.println(record.key() + "=" + record.value());
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
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.kafkaBrokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "projectGroup");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(consumerProps);
    }
}
