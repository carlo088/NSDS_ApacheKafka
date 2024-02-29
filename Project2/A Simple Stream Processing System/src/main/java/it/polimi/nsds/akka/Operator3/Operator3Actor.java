package it.polimi.nsds.akka.Operator3;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import it.polimi.nsds.akka.Messages.CrashMessage;
import it.polimi.nsds.akka.Messages.DataMessage;
import it.polimi.nsds.akka.ResumeException;

import java.util.ArrayList;
import java.util.List;

public class Operator3Actor extends AbstractActor {
    // window configuration
    final int window_size = 2;
    final int window_slide = 1;
    List<Integer> window = new ArrayList<>();

    // find the sink of the pipeline (it's necessary to know the host address)
    final private ActorSelection sink = getContext().actorSelection("akka://StreamProcessingSystem@127.0.0.1:2554/user/sinkSupervisor/sink");

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
            int aggregatedValue = computeSum(window);
            sink.tell(new DataMessage(message.getKey(), aggregatedValue), ActorRef.noSender());

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
        System.out.println("Instance " + self() + " of the operator 3 crashed due to: " + message.getError());
        throw new ResumeException();
    }

    /**
     * Operator 3's aggregation function (compute the sum)
     * @param values values of the window
     * @return sum
     */
    int computeSum(List<Integer> values){
        int sum = 0;
        for (Integer value : values) {
            sum += value;
        }
        return sum;
    }

    /**
     * Props settings
     * @return Operator 3 props
     */
    static Props props(){
        return Props.create(Operator3Actor.class);
    }

}
