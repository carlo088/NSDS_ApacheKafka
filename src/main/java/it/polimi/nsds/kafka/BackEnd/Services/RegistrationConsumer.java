package it.polimi.nsds.kafka.BackEnd.Services;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class RegistrationConsumer extends Thread{
    private final Map<String, String> db_courses;

    public RegistrationConsumer(Map<String, String> db_courses){
        this.db_courses = db_courses;
    }

    public void run(){
        // update db_courses on Registration service
        final KafkaConsumer<String, String> courseConsumer = setRegistrationConsumer();
        courseConsumer.subscribe(Collections.singleton("courses"));

        while (true){
            final ConsumerRecords<String, String> courseRecords = courseConsumer.poll(Duration.of(1, ChronoUnit.MINUTES));
            for (final ConsumerRecord<String, String> record : courseRecords){
                db_courses.put(record.key(), record.value());
            }
        }
    }

    /**
     * Kafka settings for Registration Consumer
     * @return Registration Consumer
     */
    private static KafkaConsumer<String, String> setRegistrationConsumer(){
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "registrationGroup");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true));
        return new KafkaConsumer<>(consumerProps);
    }
}
