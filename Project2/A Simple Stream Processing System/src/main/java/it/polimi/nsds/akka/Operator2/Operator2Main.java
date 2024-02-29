package it.polimi.nsds.akka.Operator2;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import it.polimi.nsds.akka.RouterActor;

public class Operator2Main {
    public static void main(String[] args) {

        // load the config file (change this file to host the operator on a different machine)
        Config config = ConfigFactory.load("operator2.conf");

        // create the system where actors have to be created
        ActorSystem system = ActorSystem.create("StreamProcessingSystem", config);

        // create the router for the operator2 instances
        system.actorOf(Props.create(RouterActor.class, Operator2Actor.props()), "router2");
    }
}
