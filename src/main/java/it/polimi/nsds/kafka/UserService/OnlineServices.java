package it.polimi.nsds.kafka.UserService;

import it.polimi.nsds.kafka.ProjectService.ProjectService;
import it.polimi.nsds.kafka.RegistrationService.RegistrationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
        int port = args.length > 0 ? parseInt(args[0]) : 7268;

        // set the server socket
        ServerSocket serverSocket = new ServerSocket(port);

        // initialize Kafka consumer used for recovering
        recoverConsumer = Utils.setConsumer();

        // TODO: initialize all services, possibly recovering data from Kafka
        UserService userService = new UserService(recover("users"));
        ProjectService projectService = new ProjectService(recover("projects"));
        RegistrationService registrationService = new RegistrationService();


        System.out.println("OnlineServices listening on port: " + port);
        while(true){
            try {
                // accept a socket and run a thread for that client connection
                Socket socket = serverSocket.accept();
                //TODO: pass to connection the classes of services
                Connection connection = new Connection(socket, userService, projectService);
                executor.submit(connection);
                System.out.println("New connection established");
            } catch (IOException e){
                System.err.println("Connection error!");
            }
        }
    }

    private static Map<String, String> recover(String topic){
        Map<String, String> db_recovered = new HashMap<>();

        recoverConsumer.subscribe(Collections.singletonList(topic));
        final ConsumerRecords<String, String> records = recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

        for(final ConsumerRecord<String, String> record : records){
            db_recovered.put(record.key(), record.value());
        }

        recoverConsumer.unsubscribe();
        return db_recovered;
    }

}
