package it.polimi.nsds.kafka.BackEnd.Services;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Beans.Project;
import it.polimi.nsds.kafka.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Random;

public class CourseService {
    private final Map<String, String> db_courses;
    private final Map<String, String> db_projects;

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

    public String newProject(String projectJson){
        // get a Project class from a Json file
        Gson gson = new Gson();
        Project project = gson.fromJson(projectJson, Project.class);

        /*
            QUI VA AGGIUNTO IL CONTROLLO SULL'ESISTENZA DEL CORSO TODO:
            BISOGNA IMPLEMENTARE UN CONSUMER SUL COURSESERVICE CHE AGGIORNA IL PROPRIO DB CON LA LISTA DEI PROJECTS
        */

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
        db_projects.put(id, gson.toJson(project));

        final ProducerRecord<String, String> projectRecord = new ProducerRecord<>("projects", id, gson.toJson(project));
        projectProducer.send(projectRecord);
        return "Project " + id + " correctly posted";
    }

    public String showCourseProjects(String courseId){
        Gson gson = new Gson();
        String response = "";
        for (String projectJson: db_projects.values()) {
            Project project = gson.fromJson(projectJson, Project.class);
            if(project.getCourseId().equals(courseId))
                response += projectJson + " ";
        }
        return response;
    }

}
