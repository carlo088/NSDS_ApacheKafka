package it.polimi.nsds.kafka.CourseService;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

public class CourseService {
    private static Map<String, Integer> db_courses;

    // kafka producer
    private KafkaProducer<String, Integer> courseProducer = null;

    public CourseService(Map<String, Integer> db_courses) {
        this.db_courses = db_courses;
        final Properties propsProd = new Properties();
        propsProd.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsProd.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsProd.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        courseProducer = new KafkaProducer<>(propsProd);

    }

    public String newCourse(String course) {
        String[] courseInfo = course.split(" ");
        String courseCode = courseInfo[0];
        int numberOfProjects = Integer.parseInt(courseInfo[1]); // number of projects associated to certain course id
    
        if (db_courses.containsKey(courseCode))
            return "Course with code " + courseCode + " already exists";
    
        db_courses.put(courseCode, numberOfProjects);
        final ProducerRecord<String, Integer> courseRecord = new ProducerRecord<>("courses", courseCode, numberOfProjects);
        courseProducer.send(courseRecord);
        return "Course " + courseCode + "with " + numberOfProjects + "projects registered";
    }

}
