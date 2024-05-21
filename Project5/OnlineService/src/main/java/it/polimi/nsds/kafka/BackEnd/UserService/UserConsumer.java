package it.polimi.nsds.kafka.BackEnd.UserService;

import it.polimi.nsds.kafka.Utils.ConfigUtils;
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

public class UserConsumer extends Thread{

    private final Map<String, String> db_courses;

    public UserConsumer(Map<String, String> db_courses){
        this.db_courses = db_courses;
    }

    public void run(){
        // constantly update db_course on UserService with the information on Kafka records
        final KafkaConsumer<String, String> userConsumer = setUserConsumer();
        userConsumer.subscribe(Collections.singleton("courses"));

        while (true){
            final ConsumerRecords<String, String> records = userConsumer.poll(Duration.of(1, ChronoUnit.MINUTES));
            for (final ConsumerRecord<String, String> record : records){
                db_courses.put(record.key(), record.value());
            }
        }
    }

    /**
     * Kafka settings for User Consumer
     * @return User Consumer
     */
    private static KafkaConsumer<String, String> setUserConsumer(){
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.kafkaBrokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "userCoursesGroup");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true));
        return new KafkaConsumer<>(consumerProps);
    }
}
