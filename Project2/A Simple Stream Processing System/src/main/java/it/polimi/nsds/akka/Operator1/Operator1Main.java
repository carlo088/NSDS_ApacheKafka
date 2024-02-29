package it.polimi.nsds.akka.Operator1;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import it.polimi.nsds.akka.RouterActor;

public class Operator1Main {
    public static void main(String[] args) {

        // load the config file (change this file to host the operator on a different machine)
        Config config = ConfigFactory.load("operator1.conf");

        // create the system where actors have to be created
        ActorSystem system = ActorSystem.create("StreamProcessingSystem", config);

        // create the router for the operator1 instances
        system.actorOf(Props.create(RouterActor.class, Operator1Actor.props()), "router1");
    }
}
