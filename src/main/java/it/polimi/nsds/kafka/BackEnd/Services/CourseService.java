package it.polimi.nsds.kafka.BackEnd.Services;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.List;
import java.util.Properties;
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

    public String enrollCourse(String username, int courseId) {
        if (!db_users.containsKey(username))
            return "User not found";

        if (!db_courses.containsKey(courseId))
            return "Course not found";

        // Load user from db_users
        User user = gson.fromJson(db_users.get(username), User.class);
        List<String> enrolledCourses = user.getCourseIds();

        if (enrolledCourses.contains(String.valueOf(courseId)))
            return "User is already enrolled in course " + courseId;

        enrolledCourses.add(String.valueOf(courseId));

        // update the CourseService db_users value associated with the key username in the db_users map
        db_users.put(username, gson.toJson(user));

        // TODO UserService: this method only updates the CourseService database, maybe one would also update the UserSerive db?

        // TODO Send record of new enrollment such that Registration service can pull it?

        return "Enrolled in course " + courseId;
    }

}
