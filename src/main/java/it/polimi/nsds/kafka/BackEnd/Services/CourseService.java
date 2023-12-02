package it.polimi.nsds.kafka.BackEnd.Services;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Beans.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class CourseService {
    private static Map<Integer, String> db_courses;
    private static Map<String, String> db_users;

    // kafka producer
    private KafkaProducer<Integer, String> courseProducer = null;

    public CourseService(Map<Integer, String> db_courses) {
        this.db_courses = db_courses;
        final Properties propsProd = new Properties();
        propsProd.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsProd.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsProd.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        courseProducer = new KafkaProducer<>(propsProd);

    }

    public String newCourse(String courseJson) {
        // get a Course class from a Json file
        Gson gson = new Gson();
        Course course = gson.fromJson(courseJson, Course.class);
    
        if (!db_users.containsKey(course.getProfessor()))
            return "Professor username doesn't exists";

        //generate key
        Random rand = new Random();
        int id = -1;
        boolean valid = false;
        while(!valid){
            id = rand.nextInt();
            if(!db_courses.containsKey(id))
                valid = true;
        }

        course.setId(id);
        db_courses.put(id, gson.toJson(course));
        final ProducerRecord<Integer, String> courseRecord = new ProducerRecord<>("courses", id, gson.toJson(course));
        courseProducer.send(courseRecord);
        return "Course added correctly";
    }

}