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
public class Subscribe extends CommonContext {

    public static int BATCH_SIZE;
    public static int MAX_RETRIES = 3;
    public static String ERROR_REPLAY_ID_VALIDATION_FAILED = "fetch.replayid.validation.failed";
    public static String ERROR_REPLAY_ID_INVALID = "fetch.replayid.corrupted";
    public static String ERROR_SERVICE_UNAVAILABLE = "service.unavailable";
    public static int SERVICE_UNAVAILABLE_WAIT_BEFORE_RETRY_SECONDS = 5;
    public static ExampleConfigurations exampleConfigurations;
    public static AtomicBoolean isActive = new AtomicBoolean(false);
    public static AtomicInteger retriesLeft = new AtomicInteger(MAX_RETRIES);
    private StreamObserver<FetchRequest> serverStream;
    private Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private AtomicInteger receivedEvents = new AtomicInteger(0);
    private final StreamObserver<FetchResponse> responseStreamObserver;
    private final ReplayPreset replayPreset;
    private final ByteString customReplayId;
    private final ExecutorService eventProcessingExecutors;
    private final ScheduledExecutorService retryScheduler;
    private volatile ByteString storedReplay;

    public Subscribe(ExampleConfigurations exampleConfigurations) {
        super(exampleConfigurations);
        isActive.set(true);
        this.exampleConfigurations = exampleConfigurations;
        this.BATCH_SIZE = Math.min(5, exampleConfigurations.getNumberOfEventsToSubscribe());
        this.responseStreamObserver = getDefaultResponseStreamObserver();
        this.setupTopicDetails(exampleConfigurations.getTopic(), false, false);
        this.replayPreset = exampleConfigurations.getReplayPreset();
        this.customReplayId = exampleConfigurations.getReplayId();
        this.retryScheduler = Executors.newScheduledThreadPool(1);
        this.eventProcessingExecutors = Executors.newFixedThreadPool(3);
    }

    public Subscribe(ExampleConfigurations exampleConfigurations, StreamObserver<FetchResponse> responseStreamObserver) {
        super(exampleConfigurations);
        isActive.set(true);
        this.exampleConfigurations = exampleConfigurations;
        this.BATCH_SIZE = Math.min(5, exampleConfigurations.getNumberOfEventsToSubscribe());
        this.responseStreamObserver = responseStreamObserver;
        this.setupTopicDetails(exampleConfigurations.getTopic(), false, false);
        this.replayPreset = exampleConfigurations.getReplayPreset();
        this.customReplayId = exampleConfigurations.getReplayId();
        this.retryScheduler = Executors.newScheduledThreadPool(1);
        this.eventProcessingExecutors = Executors.newFixedThreadPool(3);
    }

    /**
     * Function to start the subscription.
     */
    public void startSubscription() {
        logger.info("Subscription started for topic: " + busTopicName + ".");
        fetch(BATCH_SIZE, busTopicName, replayPreset, customReplayId);
        // Thread being blocked here for demonstration of this specific example. Blocking the thread in production is not recommended.
        while(isActive.get()) {
            waitInMillis(5_000);
            logger.info("Subscription Active. Received " + receivedEvents.get() + " events.");
        }
    }

    /** Helper function to send FetchRequests.
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
                        // Unless the schema of the event is available in the local schema cache, there is a blocking
                        // GetSchema call being made to obtain the schema which may block the thread. Therefore, the
                        // processing of events is done asynchronously.
                        CompletableFuture.runAsync(new EventProcessor(ce), eventProcessingExecutors);
                    } catch (Exception e) {
                        logger.info(e.toString());
                    }
                    receivedEvents.addAndGet(1);
                }
                // Latest replayId stored for any future FetchRequests with CUSTOM ReplayPreset.
                // NOTE: Replay IDs are opaque in nature and should be stored and used as bytes without any conversion.
                storedReplay = fetchResponse.getLatestReplayId();

                // Reset retry count
                if (retriesLeft.get() != MAX_RETRIES) {
                    retriesLeft.set(MAX_RETRIES);
                }

                // Implementing a basic flow control strategy where the next fetchRequest is sent only after the
                // requested number of events in the previous fetchRequest(s) are received.
                // NOTE: This block may need to be implemented before the processing of events if event processing takes
                // a long time. There is a 70s timeout period during which, if pendingNumRequested is 0 and no events are
                // further requested then the stream will be closed.
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

                    // Closing the old stream for sanity
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
                        retryDelay = getBackoffWaitTime();
                        if (storedReplay != null) {
                            logger.info("Retrying with Stored Replay.");
                            retryReplayPreset = ReplayPreset.CUSTOM;
                            retryReplayId = getStoredReplay();
                        } else {
                            logger.info("Retrying with LATEST Replay.");;
                        }

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

    /**
     * A Runnable class that is used to send the FetchRequests by making a new Subscribe call while retrying on
     * receiving an error. This is done in order to avoid blocking the thread while waiting for retries. This class is
     * passed to the ScheduledExecutorService which will asynchronously send the FetchRequests during retries.
     */
    private class RetryRequestSender implements Runnable {
        private ReplayPreset retryReplayPreset;
        private ByteString retryReplayId;
        public RetryRequestSender(ReplayPreset replayPreset, ByteString replayId) {
            this.retryReplayPreset = replayPreset;
            this.retryReplayId = replayId;
        }

        @Override
        public void run() {
            fetch(BATCH_SIZE, busTopicName, retryReplayPreset, retryReplayId);
            logger.info("Retry FetchRequest Sent.");
        }
    }

    /**
     * A Runnable class that is used to process the events asynchronously using CompletableFuture.
     */
    private class EventProcessor implements Runnable {
        private ConsumerEvent ce;
        public EventProcessor(ConsumerEvent consumerEvent) {
            this.ce = consumerEvent;
        }

        @Override
        public void run() {
            try {
                processEvent(ce);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
    public AtomicInteger getReceivedEvents() {
        return receivedEvents;
    }

    public void updateReceivedEvents(int delta) {
        receivedEvents.addAndGet(delta);
    }

    public int getBatchSize() {
        return BATCH_SIZE;
    }
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
            if (retryScheduler != null) {
                retryScheduler.shutdown();
            }
            if (eventProcessingExecutors != null) {
                eventProcessingExecutors.shutdown();
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
        } catch (Exception e) {
            printStatusRuntimeException("Error during Subscribe", e);
        }
    }
}
