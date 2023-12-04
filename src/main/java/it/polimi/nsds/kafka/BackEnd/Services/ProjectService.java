package it.polimi.nsds.kafka.BackEnd.Services;

import java.util.Map;
import java.util.Properties;
import java.util.Random;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Beans.Project;
import it.polimi.nsds.kafka.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProjectService{

    // kafka producer
    private final KafkaProducer<String, String> projectProducer;

    private final Map<String, String> db_projects;

    public ProjectService(Map<String, String> db_projects) {
        this.db_projects = db_projects;
        projectProducer = Utils.setProducer();
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

    public String submitNewSolution(String solutionJson){

    }

}