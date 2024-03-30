package it.polimi.nsds.akka.Operator2;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import it.polimi.nsds.akka.ConfigUtils;
import it.polimi.nsds.akka.Messages.CrashMessage;
import it.polimi.nsds.akka.Messages.DataMessage;
import it.polimi.nsds.akka.ResumeException;

import java.util.ArrayList;
import java.util.List;

public class Operator2Actor extends AbstractActor{
    // window configuration
    final int window_size = 4;
    final int window_slide = 2;
    List<Integer> window = new ArrayList<>();

    // find the router of the next operator (it's necessary to know the host address)
    final private ActorSelection nextRouter = getContext().actorSelection("akka://StreamProcessingSystem@" + ConfigUtils.operator3Host + ":2553/user/router3");

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
    public Receive createReceive(){
        return receiveBuilder()
                .match(DataMessage.class, this::onDataReceived)
                .match(CrashMessage.class, this::onCrashReceived)
                .build();
    }

    /**
     * Adds a value to the window and computes the aggregated value when window is completed
     * @param message data message
     */
    void onDataReceived(DataMessage message){
        // add the received value to the window
        window.add(message.getValue());
        System.out.println(self() + " received: " + message.getKey() + ", " + message.getValue());

        // if window is completed compute the aggregated value and send the result to the next router
        if (window.size() >= window_size){
            int aggregatedValue = computeMax(window);
            nextRouter.tell(new DataMessage(message.getKey(), aggregatedValue), ActorRef.noSender());

            // slide the window
            List<Integer> newWindow = new ArrayList<>();
            for (int i = window_slide; i < window_size; i++){
                newWindow.add(window.get(i));
            }
            window = newWindow;
        }
    }

    /**
     * Throws a ResumeException when the operator receives a crash message
     * @param message crash message
     * @throws ResumeException ResumeException
     */
    void onCrashReceived(CrashMessage message) throws ResumeException {
        System.out.println("Instance " + self() + " of the operator 2 crashed due to: " + message.getError());
        throw new ResumeException();
    }

    /**
     * Operator 2's aggregation function (compute the maximum)
     * @param values values of the window
     * @return maximum
     */
    int computeMax(List<Integer> values){
        int max = -1;
        for (Integer value : values) {
            if (value > max)
                max = value;
        }
        return max;
    }

    /**
     * Props settings
     * @return Operator 2 props
     */
    static Props props(){
        return Props.create(Operator2Actor.class);
    }
}
