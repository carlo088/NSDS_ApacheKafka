package it.polimi.nsds.akka;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.japi.pf.DeciderBuilder;
import it.polimi.nsds.akka.Messages.CrashMessage;
import it.polimi.nsds.akka.Messages.DataMessage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Actor class for routing messages between different instances of the same operator
 */
public class RouterActor extends AbstractActor {
    private final List<ActorRef> operatorInstances = new ArrayList<>();

    // ------- Cluster initialization --------

    Cluster cluster = Cluster.get(getContext().getSystem());

    @Override
    public void preStart(){
        cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(), ClusterEvent.MemberEvent.class, ClusterEvent.UnreachableMember.class);
    }

    @Override
    public void postStop() {
        cluster.unsubscribe(getSelf());
    }

    // ---------------------------------------

    // ------- Supervisor strategy definition --------

    /**
     * Resume an actor when it throws a ResumeException
     */
    private static final SupervisorStrategy strategy =
            new OneForOneStrategy(
                    10,
                    Duration.ofMinutes(1),
                    DeciderBuilder.match(ResumeException.class, e ->
                                    SupervisorStrategy.resume())
                            .build());
    @Override
    public SupervisorStrategy supervisorStrategy() {
        return strategy;
    }

    // ---------------------------------------



    public RouterActor(Props operatorProps){
        // create the instances of the same operator
        for(int i = 0; i < StreamProcessingSystem.NUM_OF_KEYS; i++){
            operatorInstances.add(getContext().actorOf(operatorProps, "operator_instance" + i));
        }
    }

    // when router receive a message
    @Override
    public Receive createReceive(){
        return receiveBuilder()
                .match(DataMessage.class, this::dispatch)
                .match(CrashMessage.class, this::crash)
                .build();
    }

    /**
     * Routes a DataMessage between the instances based on the key
     * @param message key-value message
     */
    private void dispatch(DataMessage message){
        operatorInstances.get(message.getKey()).tell(new DataMessage(message.getKey(), message.getValue()), self());
    }

    /**
     * Sends a CrashMessage to a random instance
     * @param message crash message
     */
    private void crash(CrashMessage message){
        Random rand = new Random();
        operatorInstances.get(rand.nextInt(operatorInstances.size())).tell(message, self());
    }

}
