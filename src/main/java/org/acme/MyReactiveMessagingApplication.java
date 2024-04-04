package org.acme;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsOutboundMetadata;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MyReactiveMessagingApplication {

    List<String> received = new CopyOnWriteArrayList<>();

    AtomicLong dedup = new AtomicLong(0);

    @Outgoing("out")
    public Multi<Message<String>> produce() {
        return Multi.createFrom().range(0, 10)
                .map(i -> Message.of(String.valueOf(i), Metadata.of(SqsOutboundMetadata.builder()
                        // .deduplicationId(dedup.incrementAndGet() + "")
                        .build())).withNack(t -> {
                            System.out.println("error ");
                            t.printStackTrace();
                            return Uni.createFrom().voidItem().subscribeAsCompletionStage();
                        }));
    }

    @Incoming("in")
    public void consume(String in) {
        Log.infof("Consumer received %s", in);
        received.add(in);
    }

    List<String> received() {
        return received;
    }

}
