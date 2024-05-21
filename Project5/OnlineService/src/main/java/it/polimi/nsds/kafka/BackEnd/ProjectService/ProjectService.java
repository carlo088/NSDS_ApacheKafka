package it.polimi.nsds.kafka.BackEnd.ProjectService;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Submission;
import it.polimi.nsds.kafka.Utils.ConfigUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;

public class ProjectService implements Runnable{
    // socket and streams
    private final Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    private boolean isActive = true;
    private final Gson gson;

    // users and courses stored in private data structures
    private final Map<String, String> db_submissions;

    // kafka producer
    private final KafkaProducer<String, String> submissionProducer;

    public ProjectService(Socket socket, Map<String, String> db_submissions){
        this.socket = socket;
        this.db_submissions = db_submissions;
        gson = new Gson();

        submissionProducer = setSubmissionProducer();
    }

    /**
     * starts the main process of the service (receiving messages from the client)
     */
    @Override
    public void run() {
        try {
            // set socket streams
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            send(new String[]{"Connection to Project Service established!"});

            // service is always waiting for requests from clients
            while(isActive){
                receive();
            }
        } catch(IOException e){
            System.err.println("Connection closed from client side");
        } catch (ClassNotFoundException e){
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

    /**
     * sends a response message to the client through the socket's output stream
     * @param message response message
     */
    public void send(String[] message){
        try {
            if (!isActive) {
                return;
            }
            out.writeObject(message);
            out.flush();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * closes the connection
     */
    public synchronized void closeConnection(){
        try{
            socket.close();
        }catch (IOException e){
            System.err.println(e.getMessage());
        }
        System.out.println("Connection closed");
        isActive = false;
    }

    /**
     * receives a request message from the client through the socket's input stream (BLOCKING FUNCTION)
     * @throws IOException if there are IO problems
     * @throws ClassNotFoundException if there are problems with readObject() method
     */
    private void receive() throws IOException, ClassNotFoundException {
        String[] request = (String[]) in.readObject();
        String requestType = request[0];

        switch(requestType){
            case "SUBMIT_NEW":
                String[] response = submitNewSolution(request[1]);
                send(response);
                break;
            case "SHOW_USER_SUBMISSIONS":
                response = checkSubmissionStatus(request[1]);
                send(response);
                break;
            case "SHOW_PROJECT_SUBMISSIONS":
                response = showNewSubmissions(request[1]);
                send(response);
                break;
            case "GRADE_SUBMISSION":
                response = updateSubmissionGrade(request[1], Integer.parseInt(request[2]));
                send(response);
                break;
            default:
                send(new String[]{""});
                break;
        }
    }

    /**
     * adds a new submission
     * @param solutionJson solution json
     * @return message for the client
     */
    public String[] submitNewSolution(String solutionJson) {
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
        }

        submission.setId(submissionId);
        db_submissions.put(submissionId, gson.toJson(submission));
        final ProducerRecord<String, String> submissionRecord = new ProducerRecord<>("submissions", submissionId, gson.toJson(submission));
        submissionProducer.send(submissionRecord);
        return new String[]{"Submission added correctly"};
    }

    /**
     * shows new submissions for a project
     * @param projectId project id
     * @return message for the client
     */
    public String[] showNewSubmissions(String projectId) {
        List<String> submissions = new ArrayList<>();
        for (String submissionJson : db_submissions.values()) {
            Submission submission = gson.fromJson(submissionJson, Submission.class);
            // Check if the submission has grade equal to -1 and matches the specified projectId
            if (submission.getGrade() == -1 && submission.getProjectId().equals(projectId))
                submissions.add(submissionJson);
        }

        String[] response = new String[submissions.size()];
        submissions.toArray(response);
        return response;
    }

    /**
     * grades a solution
     * @param submissionID submission id
     * @param grade grade
     * @return message for the client
     */
    public String[] updateSubmissionGrade(String submissionID, int grade) {
        if (db_submissions.containsKey(submissionID)) {
            String submissionJson = db_submissions.get(submissionID);
            Submission submission = gson.fromJson(submissionJson, Submission.class);

            submission.setGrade(grade);

            db_submissions.put(submissionID, gson.toJson(submission));

            final ProducerRecord<String, String> submissionRecord = new ProducerRecord<>("submissions", submissionID, gson.toJson(submission));
            submissionProducer.send(submissionRecord);

            return new String[]{"Grade updated for Submission with ID " + submissionID};
        } else {
            return new String[]{"Submission with ID " + submissionID + " not found"};
        }
    }

    /**
     * shows all the submissions of a user
     * @param studentUsername username
     * @return message for the client
     */
    public String[] checkSubmissionStatus(String studentUsername) {
        List<String> submissions = new ArrayList<>();
        for (String submissionJson : db_submissions.values()) {
            Submission submission = gson.fromJson(submissionJson, Submission.class);
            // Check if the submission belongs to the specified student
            if (submission.getStudentUsername().equals(studentUsername)) {
                submissions.add(submissionJson);
            }
        }

        String[] response = new String[submissions.size()];
        submissions.toArray(response);
        return response;
    }

    /**
     * Kafka settings for Submission Producer
     * @return Submission Producer
     */
    private static KafkaProducer<String, String> setSubmissionProducer(){
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.kafkaBrokers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }
}
