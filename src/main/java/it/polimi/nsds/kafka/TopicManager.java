package it.polimi.nsds.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class TopicManager {
    private static final String serverAddr = "localhost:9092";
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) throws Exception {
        consumer = setConsumer();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        AdminClient adminClient = AdminClient.create(props);

        ListTopicsResult listResult = adminClient.listTopics();
        Set<String> topicsNames = listResult.names().get();

        for (String topic: topicsNames) {
            System.out.println(topic);
            Map<String, String> records = getRecords(topic);
            System.out.println(records.entrySet());
        }


        Scanner in = new Scanner(System.in);
        String topic = "";

        while(!topic.equalsIgnoreCase("EXIT")){
            System.out.println("Insert a topic to delete (EXIT to quit)");
            topic = in.nextLine();

            if (topicsNames.contains(topic)) {
                System.out.println("Deleting topic " + topic + "...");
                DeleteTopicsResult delResult = adminClient.deleteTopics(Collections.singletonList(topic));
                delResult.all().get();
                // Wait for the deletion
                Thread.sleep(5000);
                System.out.println("Done!");
            }
        }
    }

    private static Map<String, String> getRecords(String topic){
        Map<String, String> recordsMap = new HashMap<>();

        consumer.subscribe(Collections.singletonList(topic));
        final ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));

        for(final ConsumerRecord<String, String> record : records){
            recordsMap.put(record.key(), record.value());
        }

        consumer.unsubscribe();
        return recordsMap;
    }

    private static KafkaConsumer<String, String> setConsumer(){
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "groupA");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(consumerProps);
    }
}
