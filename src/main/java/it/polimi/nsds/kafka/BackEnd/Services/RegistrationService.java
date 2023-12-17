package it.polimi.nsds.kafka.BackEnd.Services;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Registration;

import java.util.Map;

public class RegistrationService {
    private final Map<String, String> db_registrations;

    public RegistrationService(Map<String, String> db_registrations) {
        this.db_registrations = db_registrations;
    }

    public String showUserRegistrations(String username){
        Gson gson = new Gson();
        String response = "";
        for (String registrationJson: db_registrations.values()) {
            Registration registration = gson.fromJson(registrationJson, Registration.class);
            if(registration.getUsername().equals(username))
                response += registrationJson + " ";
        }
        return response;
    }
}
