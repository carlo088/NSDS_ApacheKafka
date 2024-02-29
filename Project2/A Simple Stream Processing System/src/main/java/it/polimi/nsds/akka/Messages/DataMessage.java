package it.polimi.nsds.akka.Messages;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Message flowing through the pipeline containing a key-value pair
 */
public class DataMessage {
    final int key;
    final int value;

    @JsonCreator
    public DataMessage(int key, int value) {
        this.key = key;
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public int getKey() {
        return key;
    }
}
