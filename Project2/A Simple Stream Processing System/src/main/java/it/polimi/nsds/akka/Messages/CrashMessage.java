package it.polimi.nsds.akka.Messages;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Message sent to cause a crash on an operator (used to simulate a fault)
 */
public class CrashMessage {
    final String error;

    @JsonCreator
    public CrashMessage(String error) {
        this.error = error;
    }

    public String getError() {
        return error;
    }
}
