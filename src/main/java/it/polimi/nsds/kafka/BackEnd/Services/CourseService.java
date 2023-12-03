package it.polimi.nsds.kafka.BackEnd.Services;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Random;

public class CourseService {
    private final Map<String, String> db_courses;

    // kafka producer
    private final KafkaProducer<String, String> courseProducer;

    public CourseService(Map<String, String> db_courses) {
        this.db_courses = db_courses;
        courseProducer = Utils.setProducer();
    }

    public String newCourse(String courseJson) {
        // get a Course class from a Json file
        Gson gson = new Gson();
        Course course = gson.fromJson(courseJson, Course.class);

        //generate key
        Random rand = new Random();
        String id = null;
        boolean valid = false;
        while(!valid){
            int randId = rand.nextInt(1001);
            id = String.valueOf(randId);
            if(!db_courses.containsKey(id))
                valid = true;
        }

        course.setId(id);
        db_courses.put(id, gson.toJson(course));
        final ProducerRecord<String, String> courseRecord = new ProducerRecord<>("courses", id, gson.toJson(course));
        courseProducer.send(courseRecord);
        return "Course added correctly";
    }

    public String showAllCourses(){
        String response = "";
        for (String courseJson: db_courses.values()) {
            response += courseJson + " ";
        }
        return response;
    }

}
