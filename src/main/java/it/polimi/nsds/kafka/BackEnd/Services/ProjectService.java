package it.polimi.nsds.kafka.BackEnd.Services;

import java.util.Map;
import java.util.Properties;
import java.util.Random;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Submission;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProjectService{

    private final KafkaProducer<String, String> submissionProducer;

    private final Map<String, String> db_submissions;

    private final Gson gson;

    public ProjectService(Map<String, String> db_submissions) {
        this.db_submissions = db_submissions;
        this.submissionProducer = setSubmissionProducer();
        this.gson = new Gson();
    }

    /**
     * adds a new submission
     * @param solutionJson solution json
     * @return message for the client
     */
    public String submitNewSolution(String solutionJson) {
        // get a submission class from a Json file
        Gson gson = new Gson();
        Submission submission = gson.fromJson(solutionJson, Submission.class);

        // search for an old submission for this course by this student
        String submissionId = null;
        for (String submissionJson : db_submissions.values()) {
            Submission sub = gson.fromJson(submissionJson, Submission.class);
            if (sub.getProjectId().equals(submission.getProjectId()) && sub.getStudentUsername().equals(submission.getStudentUsername())){
                submissionId = sub.getId();
                break;
            }
        }

        if(submissionId == null){
            // generate a key
            Random rand = new Random();
            boolean valid = false;
            while (!valid) {
                int randId = rand.nextInt(1001);
                submissionId = String.valueOf(randId);
                if (!db_submissions.containsKey(submissionId))
                    valid = true;
            }
            submission.setId(submissionId);
        }

        db_submissions.put(submissionId, gson.toJson(submission));
        final ProducerRecord<String, String> submissionRecord = new ProducerRecord<>("submissions", submissionId, gson.toJson(submission));
        submissionProducer.send(submissionRecord);
        return "Submission added correctly";
    }

    /**
     * shows new submissions for a project
     * @param projectId project id
     * @return message for the client
     */
    public String showNewSubmissions(String projectId) {
        StringBuilder response = new StringBuilder();
        for (String submissionJson : db_submissions.values()) {
            Submission submission = gson.fromJson(submissionJson, Submission.class);
            // Check if the submission has grade equal to -1 and matches the specified projectId
            if (submission.getGrade() == -1 && submission.getProjectId().equals(projectId))
                response.append(submissionJson).append(" ");
        }
        return response.toString();
    }

    /**
     * grades a solution
     * @param submissionID submission id
     * @param grade grade
     * @return message for the client
     */
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

    /**
     * shows all the submissions of a user
     * @param studentUsername username
     * @return message for the client
     */
    public String checkSubmissionStatus(String studentUsername) {
        StringBuilder response = new StringBuilder();
        for (String submissionJson : db_submissions.values()) {
            Submission submission = gson.fromJson(submissionJson, Submission.class);
            // Check if the submission belongs to the specified student
            if (submission.getStudentUsername().equals(studentUsername)) {
                response.append(submissionJson).append(" ");
            }
        }
        return response.toString();
    }

    /**
     * Kafka settings for Submission Producer
     * @return Submission Producer
     */
    private static KafkaProducer<String, String> setSubmissionProducer(){
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }

}