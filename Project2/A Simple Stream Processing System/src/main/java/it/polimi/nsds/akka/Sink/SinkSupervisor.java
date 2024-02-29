package it.polimi.nsds.akka.Sink;

import akka.actor.AbstractActor;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.japi.pf.DeciderBuilder;
import it.polimi.nsds.akka.ResumeException;

import java.time.Duration;

public class SinkSupervisor extends AbstractActor {

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


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // creates a Sink actor inside the context of Sink Supervisor
                .match(Props.class, props -> getSender().tell(getContext().actorOf(props, "sink"), getSelf()))
                .build();
    }

    /**
     * Props settings
     * @return Sink Supervisor props
     */
    public static Props props(){
        return Props.create(SinkSupervisor.class);
    }
}
