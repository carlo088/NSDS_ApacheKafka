package it.polimi.nsds.kafka.BackEnd.Services;

import java.util.Map;
import java.util.Random;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Submission;
import it.polimi.nsds.kafka.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProjectService{

    private final KafkaProducer<String, String> submissionProducer;

    private final Map<String, String> db_submissions;

    private final Gson gson;

    public ProjectService(Map<String, String> db_submissions) {
        this.db_submissions = db_submissions;
        this.submissionProducer = Utils.setProducer();
        this.gson = new Gson();
    }

    /*
        I nuovi progetti vengono gestiti da CourseService.
        Qui solo submission, il metodo newProject non serve piu
    */

    // public String newProject(String projectJson){
    //     // get a Project class from a Json file
    //     Gson gson = new Gson();
    //     Project project = gson.fromJson(projectJson, Project.class);

    //     /*
    //         QUI VA AGGIUNTO IL CONTROLLO SULL'ESISTENZA DEL CORSO TODO:
    //         BISOGNA IMPLEMENTARE UN CONSUMER SUL COURSESERVICE CHE AGGIORNA IL PROPRIO DB CON LA LISTA DEI PROJECTS
    //     */

    //     //generate key
    //     Random rand = new Random();
    //     String id = null;

    //     boolean valid = false;
    //     while(!valid){
    //         int randId = rand.nextInt(1001);
    //         id = String.valueOf(randId);
    //         if(!db_projects.containsKey(id))
    //             valid = true;
    //     }

    //     project.setId(id);
    //     db_projects.put(id, gson.toJson(project));

    //     final ProducerRecord<String, String> projectRecord = new ProducerRecord<>("projects", id, gson.toJson(project));
    //     projectProducer.send(projectRecord);
    //     return "Project " + id + " correctly posted";
    // }

    // public String showCourseProjects(String courseId){
    //     Gson gson = new Gson();
    //     String response = "";
    //     for (String projectJson: db_projects.values()) {
    //         Project project = gson.fromJson(projectJson, Project.class);
    //         if(project.getCourseId().equals(courseId))
    //             response += projectJson + " ";
    //     }
    //     return response;
    // }

    public String submitNewSolution(String solutionJson) {
        // get a submission class from a Json file
        Gson gson = new Gson();
        Submission submission = gson.fromJson(solutionJson, Submission.class);
    
        // generate a key
        Random rand = new Random();
        String submissionId = null;
        boolean valid = false;
        while (!valid) {
            int randId = rand.nextInt(1001);
            submissionId = String.valueOf(randId);
            if (!db_submissions.containsKey(submissionId))
                valid = true;
        }
    
        submission.setId(submissionId);
        db_submissions.put(submissionId, gson.toJson(submission));
        final ProducerRecord<String, String> submissionRecord = new ProducerRecord<>("submissions", submissionId, gson.toJson(submission));
        submissionProducer.send(submissionRecord);
        return "Submission added correctly";
    }

    public String showNewSubmissions(String projectId) {
        StringBuilder response = new StringBuilder();
        for (String submissionJson : db_submissions.values()) {
            Submission submission = gson.fromJson(submissionJson, Submission.class);
            // Check if the submission has grade equal to -1 and matches the specified projectId
            if (submission.getGrade() == -1 && submission.getProjectId().equals(projectId))
                response.append("ID = ").append(submission.getId()).append(" | SOLUTION = ").append(submission.getSolution()).append("\n");
        }
        return response.toString();
    }

    // In the ClientInterface.java

    // send("PROJECT_SUBMISSIONS" + " " + projectID);
    // response = receive();
    // if (response.isEmpty() || response.equals("There is no submission for this project"))
    //     System.out.println(response);
    //     return;
    // else
    //     System.out.println(response);
    
    public String updateSubmissionGrade(String submissionID, int grade) {
        if (db_submissions.containsKey(submissionID)) {
            String submissionJson = db_submissions.get(submissionID);
            Submission submission = gson.fromJson(submissionJson, Submission.class);
            
            submission.setGrade(grade);
            
            db_submissions.put(submissionID, gson.toJson(submission));
            
            final ProducerRecord<String, String> submissionRecord = new ProducerRecord<>("submissions", submissionID, gson.toJson(submission));
            submissionProducer.send(submissionRecord);
            
            return "Grade updated for Submission with ID " + submissionID;
        } else {
            return "Submission with ID " + submissionID + " not found";
        }
    }

    public String checkSubmissionStatus(String studentUsername) {
        StringBuilder response = new StringBuilder();
        for (String submissionJson : db_submissions.values()) {
            Submission submission = gson.fromJson(submissionJson, Submission.class);
            // Check if the submission belongs to the specified student
            if (submission.getStudentUsername().equals(studentUsername)) {
                response.append(submissionJson).append(" ");
            }
        }
        if (response.length() == 0) {
            return "There are no submissions";
        }
        return response.toString();
    }
    
    // simpler version

    // public String checkSubmissionStatus(String studentUsername) {
    //     StringBuilder response = new StringBuilder();
    //     for (String submissionJson : db_submissions.values()) {
    //         Submission submission = gson.fromJson(submissionJson, Submission.class);
    //         // Check if the submission belongs to the specified student
    //         if (submission.getStudentUsername().equals(studentUsername)) {
    //             response.append("Submission ID: ").append(submission.getId()).append(" | Grade: ").append(submission.getGrade()).append("\n");
    //         }
    //     }
    //     if (response.length() == 0) {
    //         return "No submissions found for student " + studentUsername;
    //     }
    //     return response.toString();
    // }

    // // ClientInterface.java
    // send("CHECK_SUBMISSION_STATUS" + " " + studentUsername);
    // response = receive();
    // System.out.println(response);
    

}