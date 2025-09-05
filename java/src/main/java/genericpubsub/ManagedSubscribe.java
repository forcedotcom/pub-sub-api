package genericpubsub;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.*;

import io.grpc.stub.StreamObserver;
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
    private static int BATCH_SIZE;
    private StreamObserver<ManagedFetchRequest> serverStream;
    private Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private final CountDownLatch serverOnCompletedLatch = new CountDownLatch(1);
    public static AtomicBoolean isActive = new AtomicBoolean(false);
    private AtomicInteger receivedEvents = new AtomicInteger(0);
    private String developerName;
    private String managedSubscriptionId;
    private final boolean processChangedFields;
    

    public ManagedSubscribe(ExampleConfigurations exampleConfigurations) {
        super(exampleConfigurations);
        isActive.set(true);
        this.managedSubscriptionId = exampleConfigurations.getManagedSubscriptionId();
        this.developerName = exampleConfigurations.getDeveloperName();
        this.BATCH_SIZE = exampleConfigurations.getNumberOfEventsToSubscribeInEachFetchRequest();
        this.processChangedFields = exampleConfigurations.getProcessChangedFields();
    }

    /**
     * Function to start the ManagedSubscription, and send first ManagedFetchRequest.
     */
    public void startManagedSubscription() {
        serverStream = asyncStub.managedSubscribe(this);
        ManagedFetchRequest.Builder builder = ManagedFetchRequest.newBuilder().setNumRequested(BATCH_SIZE);

        if (Objects.nonNull(managedSubscriptionId)) {
            builder.setSubscriptionId(managedSubscriptionId);
            logger.info("Starting managed subscription with ID {}", managedSubscriptionId);
        } else if (Objects.nonNull(developerName)) {
            builder.setDeveloperName(developerName);
            logger.info("Starting managed subscription with developer name {}", developerName);
        } else {
            logger.warn("No ID or developer name specified");
        }

        serverStream.onNext(builder.build());

        // Thread being blocked here for demonstration of this specific example. Blocking the thread in production is not recommended.
        while(isActive.get()) {
            waitInMillis(5_000);
            logger.info("Subscription Active. Received a total of " + receivedEvents.get() + " events.");
        }
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

                // Convert GenericRecord to a Map
                Map<String, Object> recordMap = new HashMap<>();
                for (Schema.Field field : writerSchema.getFields()) {
                    String fieldName = field.name();
                    Object value = record.get(fieldName);
                    recordMap.put(fieldName, value);
                }

                ObjectMapper mapper = new ObjectMapper();
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                String jsonString = mapper.writeValueAsString(recordMap);
                
                logger.info("Received event with payload:\n" + jsonString);
                logger.info("Schema name: " + writerSchema.getName());
                
                //logger.info("Received event: {}", record.toString());

                if (processChangedFields) {
                    // This example expands the changedFields bitmap field in ChangeEventHeader.
                    // To expand the other bitmap fields, i.e., diffFields and nulledFields, replicate or modify this code.
                    processAndPrintBitmapFields(writerSchema, record, "changedFields");
                }
            }
            logger.info("Processed batch of {} event(s)", response.getEventsList().size());
        }

        // Commit the replay after processing batch of events or commit the latest replay on an empty batch
        if (!response.hasCommitResponse()) {
            doCommitReplay(response.getLatestReplayId());
        }
    }

    /**
     * Helper function to commit the latest replay received from the server.
     */
    private void doCommitReplay(ByteString commitReplayId) {
        String newKey = UUID.randomUUID().toString();
        ManagedFetchRequest.Builder fetchRequestBuilder = ManagedFetchRequest.newBuilder();
        CommitReplayRequest commitRequest = CommitReplayRequest.newBuilder()
                .setCommitRequestId(newKey)
                .setReplayId(commitReplayId)
                .build();
        fetchRequestBuilder.setCommitReplayIdRequest(commitRequest);

        logger.info("Sending CommitRequest with CommitReplayRequest ID: {}" , newKey);
        serverStream.onNext(fetchRequestBuilder.build());
    }

    /**
     * Helper function to inspect the status of a commitRequest.
     */
    private void checkCommitResponse(ManagedFetchResponse fetchResponse) {
        CommitReplayResponse ce = fetchResponse.getCommitResponse();
        try {
            if (ce.hasError()) {
                logger.info("Failed Commit CommitRequestID: {} with error: {} with process time: {}",
                        ce.getCommitRequestId(), ce.getError().getMsg(), ce.getProcessTime());
                return;
            }
            logger.info("Successfully committed replay with CommitRequestId: {} with process time: {}",
                    ce.getCommitRequestId(), ce.getProcessTime());
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
            receivedEvents.addAndGet(batchSize);
            this.notifyAll();
            if (!isActive.get()) {
                return;
            }
        }

        if (fetchResponse.getPendingNumRequested() == 0) {
            fetchMore(BATCH_SIZE);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        printStatusRuntimeException("Error during subscribe stream", (Exception) throwable);

        // onError from server closes stream. notify waiting thread that subscription is no longer active.
        synchronized (this) {
            isActive.set(false);
            this.notifyAll();
        }
    }

    @Override
    public void onCompleted() {
        logger.info("Call completed by Server");
        synchronized (this) {
            isActive.set(false);
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
     * Closes the connection when the task is complete.
     */
    @Override
    public synchronized void close() {
        if (Objects.nonNull(serverStream)) {
            try {
                if (isActive.get()) {
                    isActive.set(false);
                    this.notifyAll();
                    serverStream.onCompleted();
                }
                serverOnCompletedLatch.await(6, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting to close", e);
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }
        super.close();
    }


    /**
     * Helper function to terminate the client on errors.
     */
    private synchronized void abort(Exception e) {
        serverStream.onError(e);
        isActive.set(false);
        this.notifyAll();
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

    public static void main(String args[]) throws IOException {
        ExampleConfigurations exampleConfigurations = new ExampleConfigurations("arguments.yaml");
    
        // Create an instance of ManagedSubscribe
        ManagedSubscribe subscribe = new ManagedSubscribe(exampleConfigurations);
    
        // Add a shutdown hook to close the connection when Ctrl+C is pressed
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (ManagedSubscribe.isActive.get()) {
                System.out.println("Shutdown hook triggered, closing connection...");
                subscribe.close();
            }
        }));
    
        try {
            subscribe.startManagedSubscription();
        } catch (Exception e) {
            printStatusRuntimeException("Error during ManagedSubscribe", e);
        }
    }
    
}
