package genericpubsub;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.*;

import io.grpc.stub.StreamObserver;
import utility.CommonContext;
import utility.ExampleConfigurations;

/**
 * A single-topic subscriber that consumes events using Event Bus API Subscribe RPC. The example demonstrates how to:
 * - implement a long-lived subscription to a single topic
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
    public static int MAX_RETRIES = 3;
    public static String ERROR_REPLAY_ID_VALIDATION_FAILED = "fetch.replayid.validation.failed";
    public static String ERROR_REPLAY_ID_INVALID = "fetch.replayid.corrupted";
    public static String ERROR_SERVICE_UNAVAILABLE = "service.unavailable";
    public static int SERVICE_UNAVAILABLE_WAIT_BEFORE_RETRY_SECONDS = 5;
    public static ExampleConfigurations exampleConfigurations;
    public static AtomicBoolean isActive = new AtomicBoolean(false);
    public static AtomicInteger retriesLeft = new AtomicInteger(3);
    private StreamObserver<FetchRequest> serverStream;
    private Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private AtomicInteger receivedEvents = new AtomicInteger(0);
    private final StreamObserver<FetchResponse> responseStreamObserver;
    private final ReplayPreset replayPreset;
    private final ByteString customReplayId;
    private volatile ByteString storedReplay;
    private volatile ScheduledExecutorService retryScheduler;

    public SubscribeLongLived(ExampleConfigurations exampleConfigurations) {
        super(exampleConfigurations);
        isActive.set(true);
        this.exampleConfigurations = exampleConfigurations;
        this.BATCH_SIZE = Math.min(5, exampleConfigurations.getNumberOfEventsToSubscribe());
        this.responseStreamObserver = getDefaultResponseStreamObserver();
        this.setupTopicDetails(exampleConfigurations.getTopic(), false, false);
        this.replayPreset = exampleConfigurations.getReplayPreset();
        this.customReplayId = exampleConfigurations.getReplayId();
        this.retryScheduler = Executors.newScheduledThreadPool(1);
    }

    /**
     * Function to start the subscription.
     */
    public void startSubscription() {
        logger.info("Subscription started for topic: " + busTopicName + ".");
        fetch(BATCH_SIZE, busTopicName, replayPreset, customReplayId);
        while(isActive.get()) {
            waitInMillis(5_000);
            logger.info("Received " + receivedEvents.get() + " events.");
        }
    }

    /** Helper function send FetchRequests.
     * @param providedBatchSize
     * @param providedTopicName
     * @param providedReplayPreset
     * @param providedReplayId
     */
    public void fetch(int providedBatchSize, String providedTopicName, ReplayPreset providedReplayPreset, ByteString providedReplayId) {
        serverStream = asyncStub.subscribe(this.responseStreamObserver);
        FetchRequest.Builder fetchRequestBuilder = FetchRequest.newBuilder()
                .setNumRequested(providedBatchSize)
                .setTopicName(providedTopicName)
                .setReplayPreset(providedReplayPreset);
        if (providedReplayPreset == ReplayPreset.CUSTOM) {
            logger.info("Subscription has Replay Preset set to CUSTOM. In this case, the events will be delivered from provided ReplayId.");
            fetchRequestBuilder.setReplayId(providedReplayId);
        }
        serverStream.onNext(fetchRequestBuilder.build());
    }

    /**
     * Function to decide the delay (in ms) in sending FetchRequests using
     * Binary Exponential Backoff - Waits for 2^(Max Number of Retries - Retries Left) * 1000.
     */
    public long getBackoffWaitTime() {
        long waitTime = (long) (Math.pow(2, MAX_RETRIES - retriesLeft.get()) * 1000);
        return waitTime;
    }

    /**
     * Helper function to halt the current thread.
     */
    public void waitInMillis(long duration) {
        synchronized (this) {
            try {
                this.wait(duration);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
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
                logger.info("Retries remaining: " + retriesLeft.get());
                if (retriesLeft.get() == 0) {
                    logger.info("Exhausted all retries. Closing Subscription.");
                    isActive.set(false);
                } else {
                    retriesLeft.decrementAndGet();
                    Metadata trailers = ((StatusRuntimeException)t).getTrailers() != null ? ((StatusRuntimeException)t).getTrailers() : null;
                    String errorCode = (trailers != null && trailers.get(Metadata.Key.of("error-code", Metadata.ASCII_STRING_MARSHALLER)) != null) ?
                            trailers.get(Metadata.Key.of("error-code", Metadata.ASCII_STRING_MARSHALLER)) : null;

                    serverStream.onCompleted();

                    ReplayPreset retryReplayPreset = ReplayPreset.LATEST;
                    ByteString retryReplayId = null;
                    long retryDelay = 0;

                    // Retry strategies that can be implemented based on the error type.
                    if (errorCode.contains(ERROR_REPLAY_ID_VALIDATION_FAILED) || errorCode.contains(ERROR_REPLAY_ID_INVALID)) {
                        logger.info("Invalid or no replayId provided in FetchRequest for CUSTOM Replay. Trying again with EARLIEST Replay.");
                        retryDelay = getBackoffWaitTime();
                        retryReplayPreset = ReplayPreset.EARLIEST;
                    } else if (errorCode.contains(ERROR_SERVICE_UNAVAILABLE)) {
                        logger.info("Service currently unavailable. Trying again with LATEST Replay.");
                        retryDelay = SERVICE_UNAVAILABLE_WAIT_BEFORE_RETRY_SECONDS * 1000;
                    } else {
                        logger.info("Retrying with Stored Replay.");
                        retryDelay = getBackoffWaitTime();
                        retryReplayPreset = ReplayPreset.CUSTOM;
                        retryReplayId = getStoredReplay();
                    }
                    logger.info("Retrying in " + retryDelay + "ms.");
                    retryScheduler.schedule(new RetryRequestSender(retryReplayPreset, retryReplayId), retryDelay, TimeUnit.MILLISECONDS);
                }
            }

            @Override
            public void onCompleted() {
                logger.info("Call completed by server. Closing Subscription.");
                isActive.set(false);
            }
        };
    }

    private class RetryRequestSender implements Runnable {
        ReplayPreset retryReplayPreset;
        ByteString retryReplayId;
        public RetryRequestSender(ReplayPreset replayPreset, ByteString replayId) {
            this.retryReplayPreset = replayPreset;
            this.retryReplayId = replayId;
        }
        public void run() {
            fetch(BATCH_SIZE, busTopicName, retryReplayPreset, retryReplayId);
            logger.info("Retry FetchRequest Sent.");
        }
    }

    /**
     * Helper function to process the events received.
     */
    private void processEvent(ConsumerEvent ce) throws IOException {
        Schema writerSchema = getSchema(ce.getEvent().getSchemaId());
        this.storedReplay = ce.getReplayId();
        GenericRecord record = deserialize(writerSchema, ce.getEvent().getPayload());
        logger.info("Received event with payload: " + record.toString());
    }

    /**
     * Helper function to get the schema of an event if it does not already exist in the schema cache.
     */
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
        retryScheduler.shutdown();
        super.close();
    }

    public static void main(String args[]) throws IOException  {
        ExampleConfigurations exampleConfigurations = new ExampleConfigurations("arguments.yaml");

        // Using the try-with-resource statement. The CommonContext class implements AutoCloseable in
        // order to close the resources used.
        try (SubscribeLongLived subscribe = new SubscribeLongLived(exampleConfigurations)) {
            subscribe.startSubscription();
        } catch (Exception e) {
            printStatusRuntimeException("Error during Subscribe", e);
        }
    }
}
