package it.polimi.nsds.kafka.BackEnd.OldServices;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.BackEnd.RegistrationService.RegistrationConsumer;
import it.polimi.nsds.kafka.BackEnd.RegistrationService.RegistrationProducer;
import it.polimi.nsds.kafka.Beans.Registration;

import java.util.Map;

public class OldRegistrationService {
    private final Map<String, String> db_registrations;

    public OldRegistrationService(Map<String, String> db_registrations, Map<String, String> db_courses,
                                  Map<String, String> db_submissions) {

        this.db_registrations = db_registrations;

        // start a thread for the consumer
        RegistrationConsumer consumer = new RegistrationConsumer(db_courses);
        consumer.start();

        // start a thread for the producer
        RegistrationProducer producer = new RegistrationProducer(db_registrations, db_submissions, db_courses);
        producer.start();
    }

    /**
     * shows user registrations
     * @param username username
     * @return message for the client
     */
    public String[] showUserRegistrations(String username){
        Gson gson = new Gson();
        String response = "";
        for (String registrationJson: db_registrations.values()) {
            Registration registration = gson.fromJson(registrationJson, Registration.class);
            if(registration.getUsername().equals(username))
                response += registrationJson + " ";
        }
        return response.split(" ");
    }
}
