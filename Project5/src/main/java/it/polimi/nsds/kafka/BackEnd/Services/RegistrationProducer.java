package it.polimi.nsds.kafka.BackEnd.Services;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Beans.Registration;
import it.polimi.nsds.kafka.Beans.Submission;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class RegistrationProducer extends Thread{
    private final Map<String, String> db_registrations;
    private final Map<String, String> db_submissions;
    private final Map<String, String> db_courses;

    public RegistrationProducer(Map<String, String> db_registrations, Map<String, String> db_submissions,
                                Map<String, String> db_courses){
        this.db_registrations = db_registrations;
        this.db_submissions = db_submissions;
        this.db_courses = db_courses;
    }

    public void run(){
        final KafkaProducer<String, String> registrationProducer = setRegistrationProducer();

        final KafkaConsumer<String, String> submissionConsumer = setRegistrationConsumer();
        submissionConsumer.subscribe(Collections.singleton("submissions"));

        while (true){
            final ConsumerRecords<String, String> submissionRecords = submissionConsumer.poll(Duration.of(1, ChronoUnit.MINUTES));

            // when a new submission is detected
            for (final ConsumerRecord<String, String> record : submissionRecords){
                db_submissions.put(record.key(), record.value());
                Gson gson = new Gson();
                Submission submission = gson.fromJson(record.value(), Submission.class);

                String courseId = null;
                int numOfProjects = -1;
                List<String> courseProjects = new ArrayList<>();

                // search the course and get the information
                for (String courseJson : db_courses.values()) {
                    Course course = gson.fromJson(courseJson, Course.class);
                    if(course.getProjectIds().contains(submission.getProjectId())){
                        courseId = course.getId();
                        numOfProjects = course.getProjectNum();
                        courseProjects = course.getProjectIds();
                        break;
                    }
                }

                int sum = 0;
                int submissionCount = 0;

                // search submission for the specific course and from the same student
                for (String submissionJson : db_submissions.values()) {
                    Submission sub = gson.fromJson(submissionJson, Submission.class);
                    if(courseProjects.contains(sub.getProjectId()) &&
                            sub.getStudentUsername().equals(submission.getStudentUsername())){

                        // if submission is graded, increment number of submissions and sum of the grades
                        if (sub.getGrade() != -1) {
                            submissionCount++;
                            sum += sub.getGrade();
                        }
                    }
                }

                // to register the grade, the sum must be sufficient and the number of submissions must be equal to number of projects
                if (sum >= 18 && submissionCount == numOfProjects){

                    // generate a key
                    Random rand = new Random();
                    String registrationId = null;
                    boolean valid = false;
                    while (!valid) {
                        int randId = rand.nextInt(1001);
                        registrationId = String.valueOf(randId);
                        if (!db_registrations.containsKey(registrationId))
                            valid = true;
                    }

                    Registration registration = new Registration(registrationId, submission.getStudentUsername(), courseId, sum);
                    String registrationJson = gson.toJson(registration);
                    db_registrations.put(registrationId, registrationJson);
                    final ProducerRecord<String, String> newRegistration = new ProducerRecord<>("registrations", registrationId, registrationJson);
                    registrationProducer.send(newRegistration);
                }
            }
        }
    }

    /**
     * Kafka settings for Registration Consumer
     * @return Registration Consumer
     */
    private static KafkaConsumer<String, String> setRegistrationConsumer(){
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "registrationGroup");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true));
        return new KafkaConsumer<>(consumerProps);
    }

    /**
     * Kafka settings for Registration Producer
     * @return Registration Producer
     */
    private static KafkaProducer<String, String> setRegistrationProducer(){
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }
}
