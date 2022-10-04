package genericpubsub;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.*;

import io.grpc.stub.StreamObserver;
import utility.CommonContext;
import utility.ExampleConfigurations;

/**
 * A single-topic subscriber that consumes events using Event Bus API Subscribe RPC. The example demonstrates how to
 * subscribe to a fixed number of events and ends when they are received. It also demonstrates a basic flow control
 * strategy. This example does not implement any retry logic in case of failures. 
 *
 * Example:
 * ./run.sh genericpubsub.Subscribe
 *
 * @author sidd0610
 */
public class Subscribe extends CommonContext {

    public static int BATCH_SIZE;
    public static AtomicBoolean receivedAllEvents = new AtomicBoolean(false);
    public static AtomicBoolean isActive = new AtomicBoolean(false);

    private StreamObserver<FetchRequest> serverStream;
    private Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private AtomicInteger receivedEvents = new AtomicInteger(0);
    private int totalEventsRequested;
    private StreamObserver<FetchResponse> responseStreamObserver;
    private ReplayPreset replayPreset;
    private ByteString customReplayId;
    private ByteString storedReplay;

    public Subscribe(ExampleConfigurations exampleConfigurations) {
        super(exampleConfigurations);
        subscribeConstructorHelper(exampleConfigurations, getDefaultResponseStreamObserver());
    }

    // Constructor that can be used in other examples with a custom StreamObserver.
    public Subscribe(ExampleConfigurations exampleConfigurations, StreamObserver<FetchResponse> responseStreamObserver) {
        super(exampleConfigurations);
        subscribeConstructorHelper(exampleConfigurations, responseStreamObserver);
    }

    /**
     * Helper function to the constructors to initialize the member variables.
     *
     * @param exampleConfigurations
     * @param responseStreamObserver
     */
    public void subscribeConstructorHelper(ExampleConfigurations exampleConfigurations, StreamObserver<FetchResponse> responseStreamObserver) {
        receivedAllEvents.set(false);
        isActive.set(true);
        Integer numberOfEvents = exampleConfigurations.getNumberOfEventsToSubscribe();
        this.totalEventsRequested = (numberOfEvents == null || numberOfEvents == 0) ? 100 : numberOfEvents;
        this.BATCH_SIZE = Math.min(5, exampleConfigurations.getNumberOfEventsToSubscribe());
        this.responseStreamObserver = responseStreamObserver;
        this.setupTopicDetails(exampleConfigurations.getTopic(), false, false);
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
            fetchRequestBuilder.setReplayId(customReplayId);
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
                logger.info("Received batch of " + fetchResponse.getEventsList().size() + " events");
                logger.info("RPC ID: " + fetchResponse.getRpcId());
                for(ConsumerEvent ce : fetchResponse.getEventsList()) {
                    try {
                        processEvent(ce);
                    } catch (Exception e) {
                        logger.info(e.toString());
                    }
                    receivedEvents.addAndGet(1);
                }
                storedReplay = fetchResponse.getLatestReplayId();

                if (receivedEvents.get() >= totalEventsRequested) {
                    receivedAllEvents.set(true);
                }

                // Implementing a basic flow control strategy where the next fetchRequest is sent only after the
                // requested number of events in the previous fetchRequest(s) are received.
                if (fetchResponse.getPendingNumRequested() == 0 && !receivedAllEvents.get()) {
                    fetchMore(Math.min(BATCH_SIZE, totalEventsRequested-receivedEvents.get()));
                }
            }

            @Override
            public void onError(Throwable t) {
                printStatusRuntimeException("Error during Subscribe", (Exception) t);
                isActive.set(false);
                // Retry logic should be added here if needed as this example does not demonstrate retries in case of failures.
            }

            @Override
            public void onCompleted() {
                logger.info("Received requested number of " + totalEventsRequested + " events! Call completed by server.");
                isActive.set(false);
            }
        };
    }

    private void processEvent(ConsumerEvent ce) throws IOException {
        Schema writerSchema = getSchema(ce.getEvent().getSchemaId());
        this.storedReplay = ce.getReplayId();
        GenericRecord record = deserialize(writerSchema, ce.getEvent().getPayload());
        logger.info("Received event with payload: " + record.toString());
    }

    public Schema getSchema(String schemaId) {
        return schemaCache.computeIfAbsent(schemaId, id -> {
            SchemaRequest request = SchemaRequest.newBuilder().setSchemaId(id).build();
            String schemaJson = blockingStub.getSchema(request).getSchemaJson();
            return (new Schema.Parser()).parse(schemaJson);
        });
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
        while(!receivedAllEvents.get() && isActive.get()) {
            final long elapsed = Duration.between(startTime, Instant.now()).getSeconds();

            if (elapsed > maxTimeout) {
                logger.info("Exceeded timeout of " + maxTimeout + " while waiting for events. Received " + receivedEvents.get() + " events. Exiting.");
                return;
            }

            logger.info("Subscription Active. Received " + receivedEvents.get() + " events.");
            if (receivedEvents.get() < totalEventsRequested) {
                this.wait(5_000);
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

    /**
     * Closes the connection when the task is complete.
     */
    @Override
    public synchronized void close() {
        try {
            if (serverStream != null) {
                serverStream.onCompleted();
            }
        } catch (Exception e) {
            logger.info(e.toString());
        }
        super.close();
    }

    public static void main(String args[]) throws IOException  {
        ExampleConfigurations exampleConfigurations = new ExampleConfigurations("arguments.yaml");

        // Using the try-with-resource statement. The CommonContext class implements AutoCloseable in
        // order to close the resources used.
        try (Subscribe subscribe = new Subscribe(exampleConfigurations)) {
            subscribe.startSubscription();
            subscribe.waitForEvents();
        } catch (Exception e) {
            printStatusRuntimeException("Error during Subscribe", e);
        }
    }
}
