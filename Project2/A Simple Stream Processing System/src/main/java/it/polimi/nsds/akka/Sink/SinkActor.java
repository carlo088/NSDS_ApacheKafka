package it.polimi.nsds.akka.Sink;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import it.polimi.nsds.akka.Messages.CrashMessage;
import it.polimi.nsds.akka.Messages.DataMessage;
import it.polimi.nsds.akka.ResumeException;

/**
 * Actor class for receiving the results of the pipeline
 */
public class SinkActor extends AbstractActor{

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

    // when receive a message
    @Override
    public AbstractActor.Receive createReceive(){
        return receiveBuilder()
                .match(DataMessage.class, this::onDataReceived)
                .match(CrashMessage.class, this::onCrashReceived)
                .build();
    }

    /**
     * Prints the results of the pipeline
     * @param message data message
     */
    void onDataReceived(DataMessage message){
        System.out.println(message.getKey() + ": " + message.getValue());
    }

    /**
     * Throws a ResumeException when the sink receives a crash message
     * @param message crash message
     * @throws ResumeException ResumeException
     */
    void onCrashReceived(CrashMessage message) throws ResumeException {
        System.out.println("The sink of the pipeline crashed due to: " + message.getError());
        throw new ResumeException();
    }

    /**
     * Props settings
     * @return Sink props
     */
    public static Props props(){
        return Props.create(SinkActor.class);
    }
}
