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
 * A single-topic subscriber that consumes events using Event Bus API Subscribe RPC. The example demonstrates how to:
 * - implement a long lived subscription to a single topic
 * - a basic flow control strategy
 * - a basic retry strategy.
 *
 * Example:
 * ./run.sh genericpubsub.Subscribe
 *
 * @author sidd0610
 */
public class SubscribeLongLived extends CommonContext {

    public static int BATCH_SIZE;
    public static AtomicBoolean isActive = new AtomicBoolean(false);
    public static AtomicInteger numOfRetries = new AtomicInteger(3);
    private StreamObserver<FetchRequest> serverStream;
    private Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private AtomicInteger receivedEvents = new AtomicInteger(0);
    //    private int totalEventsRequested;
    private StreamObserver<FetchResponse> responseStreamObserver;
    private ReplayPreset replayPreset;
    private ByteString customReplayId;
    private ByteString storedReplay;

    public SubscribeLongLived(ExampleConfigurations exampleConfigurations) {
        super(exampleConfigurations);
        subscribeConstructorHelper(exampleConfigurations, getDefaultResponseStreamObserver());
    }

    /**
     * Helper function to the constructors to initialize the member variables.
     *
     * @param exampleConfigurations
     * @param responseStreamObserver
     */
    public void subscribeConstructorHelper(ExampleConfigurations exampleConfigurations, StreamObserver<FetchResponse> responseStreamObserver) {
//        receivedAllEvents.set(false);
        isActive.set(true);
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
            logger.info("Subscription has Replay Preset set to CUSTOM. In this case, the events will be delivered from provided ReplayId.");
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
                // Latest replayId stored for any future FetchRequests with CUSTOM ReplayPreset.
                storedReplay = fetchResponse.getLatestReplayId();

                // Implementing a basic flow control strategy where the next fetchRequest is sent only after the
                // requested number of events in the previous fetchRequest(s) are received.
                if (fetchResponse.getPendingNumRequested() == 0) {
                    fetchMore(BATCH_SIZE);
                }
            }

            @Override
            public void onError(Throwable t) {
                printStatusRuntimeException("Error during Subscribe", (Exception) t);
                if (numOfRetries.getAndDecrement() == 0) {
                    isActive.set(false);
                } else {
                    logger.info("Resubscribing from last stored replayId.");
                    serverStream = asyncStub.subscribe(responseStreamObserver);
                    FetchRequest.Builder retryFetchRequestBuilder = FetchRequest.newBuilder()
                            .setNumRequested(BATCH_SIZE)
                            .setTopicName(busTopicName)
                            .setReplayPreset(ReplayPreset.CUSTOM)
                            .setReplayId(getStoredReplay());
                    serverStream.onNext(retryFetchRequestBuilder.build());
                    logger.info("Retry FetchRequest Sent. Retries remaining: " + numOfRetries);
                }
            }

            @Override
            public void onCompleted() {
                logger.info("Call completed by server. Closing Subscription.");
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
        while(isActive.get()) {
            final long elapsed = Duration.between(startTime, Instant.now()).getSeconds();

            if (elapsed > maxTimeout) {
                logger.info("Exceeded timeout of " + maxTimeout + " while waiting for events. Received " + receivedEvents.get() + " events. Exiting.");
                return;
            }

            logger.info("Subscription Active. Received " + receivedEvents.get() + " events.");
            this.wait(5_000);
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
    public ByteString getStoredReplay() {
        return storedReplay;
    }

    public void setStoredReplay(ByteString storedReplay) {
        this.storedReplay = storedReplay;
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
        try (SubscribeLongLived subscribe = new SubscribeLongLived(exampleConfigurations)) {
            subscribe.startSubscription();
            subscribe.waitForEvents();
        } catch (Exception e) {
            printStatusRuntimeException("Error during Subscribe", e);
        }
    }
}
