package it.polimi.nsds.kafka.BackEnd.OldServices;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Beans.Project;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class OldCourseService {
    private final Map<String, String> db_courses;
    private final Map<String, String> db_projects;

    // kafka producer
    private final KafkaProducer<String, String> courseProducer;

    public OldCourseService(Map<String, String> db_courses, Map<String, String> db_projects) {
        this.db_courses = db_courses;
        this.db_projects = db_projects;
        courseProducer = setCourseProducer();
    }

    /**
     * adds a new course
     * @param courseJson course json
     * @return message for the client
     */
    public String[] newCourse(String courseJson) {
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
        return new String[]{"Course added correctly"};
    }

    /**
     * removes a course
     * @param courseId course id
     * @return message for the client
     */
    public synchronized String[] removeCourse(String courseId){
        if (!db_courses.containsKey(courseId))
            return new String[]{"Course doesn't exists"};

        db_courses.remove(courseId);

        //TODO: rimuovere record su Kafka e aggionrare db in UserService
        return new String[]{"Course removed correctly"};
    }

    /**
     * shows all existing courses
     * @return all the courses in json format
     */
    public String[] showAllCourses(){
        String response = "";
        for (String courseJson: db_courses.values()) {
            response += courseJson + " ";
        }
        return response.split(" ");
    }

    /**
     * create a new project and add it to the course list
     * @param projectJson project json
     * @return message for the client
     */
    public synchronized String[] newProject(String projectJson){
        // get a Project class from a Json file
        Gson gson = new Gson();
        Project project = gson.fromJson(projectJson, Project.class);

        if(!db_courses.containsKey(project.getCourseId()))
            return new String[]{"Course doesn't exists"};

        Course course = gson.fromJson(db_courses.get(project.getCourseId()), Course.class);
        if (course.getProjectIds().size() == course.getProjectNum())
            return new String[]{"You can't add more than " + course.getProjectNum() + " projects to this course"};

        //generate key
        Random rand = new Random();
        String id = null;

        boolean valid = false;
        while(!valid){
            int randId = rand.nextInt(1001);
            id = String.valueOf(randId);
            if(!db_projects.containsKey(id))
                valid = true;
        }

        project.setId(id);
        course.getProjectIds().add(id);

        // update course list on local db and Kafka
        db_courses.put(course.getId(), gson.toJson(course));
        final ProducerRecord<String, String> courseRecord = new ProducerRecord<>("courses", course.getId(), gson.toJson(course));
        courseProducer.send(courseRecord);

        // public project on local db and Kafka
        db_projects.put(project.getId(), gson.toJson(project));
        final ProducerRecord<String, String> projectRecord = new ProducerRecord<>("projects", id, gson.toJson(project));
        courseProducer.send(projectRecord);
        return new String[]{"Project correctly posted"};
    }

    /**
     * shows all the projects for a course
     * @param courseId course id
     * @return message for the client
     */
    public String[] showCourseProjects(String courseId){
        Gson gson = new Gson();
        List<String> projects = new ArrayList<>();
        Course course = gson.fromJson(db_courses.get(courseId), Course.class);

        for (String projectId: course.getProjectIds()) {
            String projectJson = db_projects.get(projectId);
            projects.add(projectJson);
        }

        String[] response = new String[projects.size()];
        projects.toArray(response);
        return response;
    }

    /**
     * Kafka settings for Course Producer
     * @return Course Producer
     */
    private static KafkaProducer<String, String> setCourseProducer(){
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }
}
