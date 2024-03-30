package it.polimi.nsds.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import it.polimi.nsds.akka.Messages.CrashMessage;
import it.polimi.nsds.akka.Messages.DataMessage;
import it.polimi.nsds.akka.Sink.SinkActor;
import it.polimi.nsds.akka.Sink.SinkSupervisor;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import static akka.pattern.Patterns.ask;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StreamProcessingSystem {
    // number of data types, identified by a different key
    public static final int NUM_OF_KEYS = 3;

    public static void main(String[] args) throws IOException {

        // load the config file (change this file to host the operator on a different machine)
        Config config = ConfigFactory.load("streamProcess.conf");

        // create the system where actors have to be created
        ActorSystem system = ActorSystem.create("StreamProcessingSystem", config);

        // find the routers of the operators (it's necessary to know the host address)
        ActorSelection router1 = system.actorSelection("akka://StreamProcessingSystem@" + ConfigUtils.operator1Host + ":2551/user/router1");
        ActorSelection router2 = system.actorSelection("akka://StreamProcessingSystem@" + ConfigUtils.operator2Host + ":2552/user/router2");
        ActorSelection router3 = system.actorSelection("akka://StreamProcessingSystem@" + ConfigUtils.operator3Host + ":2553/user/router3");

        try {

            // create a Sink Supervisor actor
            final ActorRef sinkSupervisor = system.actorOf(SinkSupervisor.props(), "sinkSupervisor");

            // create a Sink actor inside the context of its supervisor with the ask pattern
            Future<Object> waitingForSink = ask(sinkSupervisor, SinkActor.props(), 5000);
            final ActorRef sinkActor = (ActorRef) waitingForSink.result(Duration.create(5, SECONDS), null);


            // ------- Stream processes definition --------

            // read the data from inputStream.txt
            File streamData = new File("src/main/resources/inputStreams/noCrashes.txt");
            SECONDS.sleep(3);

            System.out.println("Starting process...");
            Scanner scanner = new Scanner(streamData);
            while (scanner.hasNextLine()) {
                String data = scanner.nextLine();
                switch (data){
                    case "crash\t1":
                        router1.tell(new CrashMessage("Crash test on operator 1"), ActorRef.noSender());
                        break;
                    case "crash\t2":
                        router2.tell(new CrashMessage("Crash test on operator 2"), ActorRef.noSender());
                        break;
                    case "crash\t3":
                        router3.tell(new CrashMessage("Crash test on operator 3"), ActorRef.noSender());
                        break;
                    case "crash\tsink":
                        sinkActor.tell(new CrashMessage("Crash test on sink"), ActorRef.noSender());
                        break;
                    default:
                        int key = Integer.parseInt(data.split("\t")[0]);
                        int value = Integer.parseInt(data.split("\t")[1]);
                        router1.tell(new DataMessage(key, value), ActorRef.noSender());
                        break;
                }
            }

            scanner.close();

        } catch (Exception e){
            e.printStackTrace();
        }

        System.in.read();
        system.terminate();
    }
}
