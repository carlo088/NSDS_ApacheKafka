package it.polimi.nsds.kafka.BackEnd.Services;

import it.polimi.nsds.kafka.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

public class UserConsumer extends Thread{

    private final Map<String, String> db_courses;

    public UserConsumer(Map<String, String> db_courses){
        this.db_courses = db_courses;
    }

    public void run(){
        final KafkaConsumer<String, String> consumer = Utils.setConsumer();
        consumer.subscribe(Collections.singleton("courses"));

        while (true){
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(1, ChronoUnit.MINUTES));
            for (final ConsumerRecord<String, String> record : records){
                db_courses.put(record.key(), record.value());
            }
        }
    }
}
