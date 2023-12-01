package it.polimi.nsds.kafka.ProjectService;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProjectService{


    // kafka producer
    private KafkaProducer<String, String> projectProducer = null;

    private static Map<String, String> db_projects;

    public ProjectService(Map<String, String> db_projects) {
        this.db_projects = db_projects;

        final Properties propsProd = new Properties();
        propsProd.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsProd.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsProd.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        projectProducer = new KafkaProducer<>(propsProd);
    }

    public String newProject(String project){
        String par[] = project.split(" ");
        String courseID = par[0];
        String projectID = par[1];
        String desc = "";
        for (int i = 2; i < par.length; i++)
            desc = desc + par[i];
 
        if (db_projects.containsKey(courseID + " " + projectID))
            return "Project " + projectID + " already exists for course: " + courseID;


        /*
            QUI VA AGGIUNTO IL CONTROLLO SULL'ESISTENZA DEL CORSO TODO:
        */
        
        db_projects.put(courseID + " " + projectID, desc);
        final ProducerRecord<String, String> newProject = new ProducerRecord<>("projects", courseID + " " + projectID, desc);
        projectProducer.send(newProject);
        return "Project " + projectID + " correctly posted";
    }

}