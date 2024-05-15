package genericpubsub;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.*;

import io.grpc.stub.StreamObserver;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.jetbrains.annotations.NotNull;
import utility.CommonContext;
import utility.ExampleConfigurations;

/**
 * A single-topic subscriber that consumes events using Event Bus API ManagedSubscribe RPC. The example demonstrates how to:
 * - implement a long-lived subscription to a single topic
 * - a basic flow control strategy
 * - a basic commits strategy.
 * <p>
 * Example:
 * ./run.sh genericpubsub.ManagedSubscribe
 *
 * @author jalaya
 */
public class ManagedSubscribe extends CommonContext implements StreamObserver<ManagedFetchResponse> {
    private static int BATCH_SIZE = 5;
    private StreamObserver<ManagedFetchRequest> serverStream;
    private Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private final CountDownLatch serverOnCompletedLatch = new CountDownLatch(1);
    private boolean isActive;
    private int receivedEvents = 0;
    private String developerName;
    private String managedSubscriptionId;

    public ManagedSubscribe(ExampleConfigurations exampleConfigurations) {
        super(exampleConfigurations);
        this.isActive= true;
        this.managedSubscriptionId = exampleConfigurations.getManagedSubscriptionId();
        this.developerName = exampleConfigurations.getDeveloperName();
        BATCH_SIZE = Math.min(5, exampleConfigurations.getNumberOfEventsToSubscribeInEachFetchRequest());
    }

    /**
     * Function to start the ManagedSubscription, and send first ManagedFetchRequest.
     */
    public void startManagedSubscription() {
        serverStream = asyncStub.managedSubscribe(this);
        ManagedFetchRequest.Builder builder = ManagedFetchRequest.newBuilder().setNumRequested(BATCH_SIZE);

        if (Objects.nonNull(developerName)) {
            builder.setDeveloperName(developerName);
        }
        if (Objects.nonNull(managedSubscriptionId)) {
            builder.setSubscriptionId(managedSubscriptionId);
        }
        serverStream.onNext(builder.build());
    }

    /**
     * Helps keep the subscription active by sending FetchRequests at regular intervals.
     *
     * @param numOfRequestedEvents
     */
    private void fetchMore(int numOfRequestedEvents) {
        logger.info("Fetching more events: {}", numOfRequestedEvents);
        ManagedFetchRequest fetchRequest = ManagedFetchRequest
                .newBuilder()
                .setNumRequested(numOfRequestedEvents)
                .build();
        serverStream.onNext(fetchRequest);
    }

    /**
     * Helper function to process the events received.
     */
    private void processEvent(ManagedFetchResponse response) throws IOException {
        if (response.getEventsCount() > 0) {
            for (ConsumerEvent event : response.getEventsList()) {
                String schemaId = event.getEvent().getSchemaId();
                logger.info("processEvent - EventID: {} SchemaId: {}", event.getEvent().getId(), schemaId);
                Schema writerSchema = getSchema(schemaId);
                GenericRecord record = deserialize(writerSchema, event.getEvent().getPayload());
                logger.info("Received event: {}", record.toString());
            }
            logger.info("Processed batch of {} event(s)", response.getEventsList().size());
        }

        // Commit the replay after processing batch of events
        if (!response.hasCommitResponse() && response.getEventsCount() > 0) {
            doCommitReplay(response.getLatestReplayId());
        }
    }

    /**
     * Helper function to commit the latest replay received from the server.
     */
    private void doCommitReplay(ByteString commitReplayId) {
        int numRequested = 1;
        String newKey =UUID.randomUUID().toString();
        ManagedFetchRequest.Builder fetchRequestBuilder = ManagedFetchRequest.newBuilder().setNumRequested(numRequested);
        CommitReplayRequest commitRequest = CommitReplayRequest.newBuilder()
                .setCommitRequestId(newKey)
                .setReplayId(commitReplayId)
                .build();
        fetchRequestBuilder.setCommitReplayIdRequest(commitRequest);

        logger.info("Sending CommitRequest with numRequested {} , CommitReplayRequest ID: {}", numRequested , newKey);
        serverStream.onNext(fetchRequestBuilder.build());
    }

    /**
     * Helper function to inspect the status of a commitRequest.
     */
    private void checkCommitResponse(@NotNull ManagedFetchResponse fetchResponse) {
        CommitReplayResponse ce = fetchResponse.getCommitResponse();
        try {
            if (ce.hasError()) {
                logger.info("Failed Commit CommitRequestID: {} with error: {}", ce.getCommitRequestId(), ce.getError().getMsg());
                return;
            }
            logger.info("Successfully committed replay with CommitRequestId: {}", ce.getCommitRequestId());
        } catch (Exception e) {
            logger.warn(e.getMessage());
            abort(new RuntimeException("Client received error. Closing Call." + e));
        }
    }

    @Override
    public void onNext(ManagedFetchResponse fetchResponse) {
        int batchSize = fetchResponse.getEventsList().size();
        logger.info("ManagedFetchResponse batch of {} events pending requested: {}", batchSize, fetchResponse.getPendingNumRequested());
        logger.info("RPC ID: {}", fetchResponse.getRpcId());

        if (fetchResponse.hasCommitResponse()) {
            checkCommitResponse(fetchResponse);
        }
        try {
            processEvent(fetchResponse);
        } catch (IOException e) {
            logger.warn(e.getMessage());
            abort(new RuntimeException("Client received error. Closing Call." + e));
        }

        synchronized (this) {
            receivedEvents += batchSize;
            this.notifyAll();
            if (!isActive) {
                return;
            }
        }
        if (fetchResponse.getPendingNumRequested() == 0) {
            fetchMore(BATCH_SIZE);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        // onError from server closes stream. notify waiting thread that subscription is no longer active.
        synchronized (this) {
            isActive = false;
            this.notifyAll();
        }
        printStatusRuntimeException("Error during subscribe stream", (Exception) throwable);
    }

    @Override
    public void onCompleted() {
        logger.info("Call completed by Server");
        synchronized (this) {
            isActive = false;
            this.notifyAll();
        }
        serverOnCompletedLatch.countDown();
    }

    /**
     * Helper function to get the schema of an event if it does not already exist in the schema cache.
     */
    private Schema getSchema(String schemaId) {
        return schemaCache.computeIfAbsent(schemaId, id -> {
            SchemaRequest request = SchemaRequest.newBuilder().setSchemaId(id).build();
            String schemaJson = blockingStub.getSchema(request).getSchemaJson();
            return (new Schema.Parser()).parse(schemaJson);
        });
    }

    /**
     * Helper function to decode the payload from the stream.
     */
    public static GenericRecord deserialize(Schema schema, ByteString payload) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        ByteArrayInputStream in = new ByteArrayInputStream(payload.toByteArray());
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
        return reader.read(null, decoder);
    }

    /**
     * Closes the connection when the task is complete.
     */
    @Override
    public synchronized void close() {
        if (Objects.nonNull(serverStream)) {
            try {
                if (isActive) {
                    isActive = false;
                    this.notifyAll();
                    serverStream.onCompleted();
                }
                serverOnCompletedLatch.await(6, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn("interrupted while waiting to close ", e);
            }
        }
        super.close();
    }

    /**
     * Helper function to terminate the client on errors.
     */
    private synchronized void abort(Exception e) {
        serverStream.onError(e);
        isActive = false;
        this.notifyAll();
    }

    /**
     *  function illustrates the statues of the stream on a fixed intervals.
     *  This is for demonstration purposes only, and not to be used in a production setting.
     */
    public synchronized boolean waitForEvents(int numEvents) {
        return waitForEventsWithMaxTimeOut(numEvents, Integer.MAX_VALUE);
    }

    public synchronized boolean waitForEventsWithMaxTimeOut(int numEvents, int timeoutInSeconds) {
        final Instant startTime = Instant.now();
        while (isActive && receivedEvents < numEvents) {
            final long elapsed = Duration.between(startTime, Instant.now()).getSeconds();

            if (elapsed > timeoutInSeconds) {
                isActive = false;
                logger.warn("Waiting for events for more than {} seconds", timeoutInSeconds);
                return receivedEvents < numEvents;
            }

            try {
                logger.info("received {} out of {}", receivedEvents, numEvents);
                this.wait(10_000);
            } catch (InterruptedException e) {
                logger.warn("interrupted while waiting", e);
                break;
            }
        }
        logger.info("Total Received Events {} out of {}", receivedEvents, numEvents);
        return receivedEvents < numEvents;
    }


    public static void main(String args[]) throws IOException  {
        ExampleConfigurations exampleConfigurations = new ExampleConfigurations("arguments.yaml");

        // Using the try-with-resource statement. The CommonContext class implements AutoCloseable in
        // order to close the resources used.
        try (ManagedSubscribe subscribe = new ManagedSubscribe(exampleConfigurations)) {
            subscribe.startManagedSubscription();
            subscribe.waitForEvents(Math.min(5, exampleConfigurations.getNumberOfEventsToSubscribeInEachFetchRequest()));
        } catch (Exception e) {
            printStatusRuntimeException("Error during ManagedSubscribe", e);
        }
    }
}
