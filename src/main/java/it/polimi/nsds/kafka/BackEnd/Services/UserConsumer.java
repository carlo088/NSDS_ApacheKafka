package it.polimi.nsds.kafka.BackEnd.Services;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Beans.User;
import it.polimi.nsds.kafka.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

public class UserConsumer extends Thread{

    private static Map<String, String> db_user;

    public UserConsumer(Map<String, String> db_user){
        this.db_user = db_user;
    }

    public void run(){
        final KafkaProducer<String, String> userProducer = Utils.setProducer();

        final KafkaConsumer<String, String> consumer = Utils.setConsumer();
        consumer.subscribe(Collections.singleton("courses"));

        while (true){
            //FIXME: questo consumer deve costantemente aggiornare la lista di corsi, anche nel caso venga ne rimosso uno
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            for (final ConsumerRecord<String, String> record : records){
                Gson gson = new Gson();
                Course course = gson.fromJson(record.value(), Course.class);
                if(db_user.containsKey(course.getProfessor())){
                    User user = gson.fromJson(db_user.get(course.getProfessor()), User.class);
                    user.addCourse(record.value());
                    db_user.put(course.getProfessor(), gson.toJson(user));

                    // update Kafka record
                    String userJson = gson.toJson(user);
                    final ProducerRecord<String, String> newUser = new ProducerRecord<>("users", user.getUsername(), userJson);
                    userProducer.send(newUser);
                }
            }
        }
    }
}
