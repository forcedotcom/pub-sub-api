package genericpubsub;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.*;

import io.grpc.stub.StreamObserver;
import utility.CommonContext;
import utility.ExampleConfigurations;

/**
 * A single-topic subscriber that consumes events using Event Bus API Subscribe RPC.
 *
 * Example:
 * ./run.sh genericpubsub.SubscribeStream
 *
 * @author sidd0610
 */
public class SubscribeStream extends CommonContext {

    public static int BATCH_SIZE;
    public static AtomicBoolean receivedAllEvents = new AtomicBoolean(false);

    private StreamObserver<FetchRequest> serverStream;
    private AtomicInteger receivedEvents = new AtomicInteger(0);
    private int totalEventsRequested;
    private StreamObserver<FetchResponse> responseStreamObserver;
    private Schema schema;
    private ReplayPreset replayPreset;
    private ByteString customReplayId;
    private ByteString storedReplay;

    public SubscribeStream(ExampleConfigurations exampleConfigurations) {
        super(exampleConfigurations);
        subscribeStreamConstructorHelper(exampleConfigurations, getDefaultResponseStreamObserver());
    }

    // Constructor that can be used in other examples with a custom StreamObserver.
    public SubscribeStream(ExampleConfigurations exampleConfigurations, StreamObserver<FetchResponse> responseStreamObserver) {
        super(exampleConfigurations);
        subscribeStreamConstructorHelper(exampleConfigurations, responseStreamObserver);
    }

    /**
     * Helper function to the constructors to initialize the member variables.
     *
     * @param exampleConfigurations
     * @param responseStreamObserver
     */
    public void subscribeStreamConstructorHelper(ExampleConfigurations exampleConfigurations, StreamObserver<FetchResponse> responseStreamObserver) {
        receivedAllEvents.set(false);
        Integer numberOfEvents = exampleConfigurations.getNumberOfEventsToSubscribe();
        this.totalEventsRequested = (numberOfEvents == null || numberOfEvents == 0) ? Integer.MAX_VALUE : numberOfEvents;
        this.BATCH_SIZE = Math.min(5, exampleConfigurations.getNumberOfEventsToSubscribe());
        this.responseStreamObserver = responseStreamObserver;
        this.setupTopicDetails(exampleConfigurations.getTopic(), false, true);
        schema = new Schema.Parser().parse(schemaInfo.getSchemaJson());
        this.replayPreset = exampleConfigurations.getReplayPreset();
        this.customReplayId = exampleConfigurations.getReplayId();
    }

    /**
     * Function to start the subscription.
     */
    public void startSubscription() {
        logger.info("Subscription started for topic: " + busTopicName + ".");
        serverStream = asyncStub.subscribe(this.responseStreamObserver);
        FetchRequest.Builder fetchRequestBuilder = FetchRequest.newBuilder()
                .setReplayPreset(replayPreset)
                .setTopicName(this.busTopicName)
                .setNumRequested(BATCH_SIZE);
        if (this.replayPreset == ReplayPreset.CUSTOM) {
            fetchRequestBuilder.setReplayId(storedReplay);
        }
        serverStream.onNext(fetchRequestBuilder.build());
    }

    /**
     * Creates a StreamObserver for handling the incoming FetchResponse messages from the server.
     *
     * @return
     */
    private StreamObserver<FetchResponse> getDefaultResponseStreamObserver() {
        return new StreamObserver<FetchResponse>() {
            @Override
            public void onNext(FetchResponse fetchResponse) {
                logger.info("Received batch of " + fetchResponse.getEventsList().size()+ " events");
                logger.info("RPC ID: " + fetchResponse.getRpcId());
                logger.info("Replay ID: " + fetchResponse.getLatestReplayId());
                for(ConsumerEvent ce : fetchResponse.getEventsList()) {
                    try {
                        logger.info(deserialize(schema, ce.getEvent().getPayload()).toString());
                    } catch (Exception e) {
                        logger.info(e.toString());
                    }
                    receivedEvents.addAndGet(1);
                }

                if (receivedEvents.get() < totalEventsRequested) {
                    storedReplay = fetchResponse.getLatestReplayId();
                    fetchMore(BATCH_SIZE);
                } else if (receivedEvents.get() >= totalEventsRequested) {
                    receivedAllEvents.set(true);
                }
            }

            @Override
            public void onError(Throwable t) {
                printStatusRuntimeException("Error during SubscribeStream", (Exception) t);
            }

            @Override
            public void onCompleted() {
                logger.info("Received requested number of events! Call completed by server.");
            }
        };
    }

    /**
     * Helps keep the subscription active by sending FetchRequests at regular intervals.
     *
     * @param numEvents
     */
    public void fetchMore(int numEvents) {
        FetchRequest fetchRequest = FetchRequest.newBuilder().setTopicName(this.busTopicName)
                .setNumRequested(numEvents).build();
        serverStream.onNext(fetchRequest);
    }

    /**
     * Main objective of this function is to keep the main thread active along with the gRPC threads.
     * The `maxTimeout` parameter is used to specify the maximum amount of time the subscription can
     * stay alive when idle, i.e., while not receiving any events.
     *
     * @param maxTimeout
     * @throws InterruptedException
     */
    public synchronized void waitForEventsWithMaxTimeout(int maxTimeout) throws InterruptedException {
        final Instant startTime = Instant.now();
        while(!receivedAllEvents.get()) {
            final long elapsed = Duration.between(startTime, Instant.now()).getSeconds();

            if (elapsed > maxTimeout) {
                logger.info("Exceeded timeout of "+ maxTimeout +" while waiting for events. Received " + receivedEvents.get() + " events. Exiting");
                return;
            }

            logger.info("Subscription Active. Received " + receivedEvents.get() + " events.");
            if (receivedEvents.get() < totalEventsRequested) {
                this.wait(10_000);
            }
        }
    }

    /**
     * Default function to wait for events indefinitely.
     *
     * @throws InterruptedException
     */
    public synchronized void waitForEvents() throws InterruptedException {
        waitForEventsWithMaxTimeout(Integer.MAX_VALUE);
    }

    /**
     * General getters and setters.
     */
    public AtomicInteger getReceivedEvents() {
        return receivedEvents;
    }

    public void updateReceivedEvents(int delta) {
        receivedEvents.addAndGet(delta);
    }

    public int getTotalEventsRequested() {
        return totalEventsRequested;
    }

    public int getBatchSize() {
        return BATCH_SIZE;
    }

    public Schema getSchema() {
        return schema;
    }

    /**
     * Closes the connection when the task is complete.
     */
    @Override
    public synchronized void close() {
        try {
            serverStream.onCompleted();
        } catch (Exception e) {
            logger.info(e.toString());
        }
        super.close();
    }

    public static void main(String args[]) throws IOException  {
        ExampleConfigurations exampleConfigurations = new ExampleConfigurations("arguments.yaml");

        // Using the try-with-resource statement. The CommonContext class implements AutoCloseable in
        // order to close the resources used.
        try (SubscribeStream subscribeStream = new SubscribeStream(exampleConfigurations)) {
            subscribeStream.startSubscription();
            subscribeStream.waitForEvents();
        } catch (Exception e) {
            logger.info(e.toString());
        }
    }
}
