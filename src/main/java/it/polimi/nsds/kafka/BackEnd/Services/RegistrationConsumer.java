package it.polimi.nsds.kafka.BackEnd.Services;

import it.polimi.nsds.kafka.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

public class RegistrationConsumer extends Thread{
    private final Map<String, String> db_registrations;

    public RegistrationConsumer(Map<String, String> db_registrations){
        this.db_registrations = db_registrations;
    }

    // quando viene aggiunta una nuova sottomissione (rilevabile tramite un consumer con aggiornamento "latest") controllo se
    // per quello studente e per quel corso sono stati consegnati tutti i progetti ed è stata raggiunta la sufficienza
    public void run(){
        final KafkaConsumer<String, String> consumer = Utils.setConsumer();
        consumer.subscribe(Collections.singleton("courses"));

        while (true){
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(1, ChronoUnit.MINUTES));
            for (final ConsumerRecord<String, String> record : records){
                db_registrations.put(record.key(), record.value());
            }
        }
    }

    /*
    questo consumer può semplicemente prendere i dati di db_course, db_project e db_submissions -> li aggiorna al servizio
    un altra classe Registration Producer si occuperà di fare: per ogni nuova sottomissione -> project = sotto.getProjId
    -> course = project.getCourseId -> numProj; lista projects con stesso courseId -> search submission con projectId e username
    -> if #risultati == numProj && somma > sufficienza -> registro
     */
}
