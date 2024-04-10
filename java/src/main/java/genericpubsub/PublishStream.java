package genericpubsub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.*;

import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import utility.CommonContext;
import utility.ExampleConfigurations;

/**
 * A single-topic publisher that creates Order Event events and publishes them. This example
 * uses Pub/Sub API's PublishStream RPC to publish events.
 *
 * Example:
 * ./run.sh genericpubsub.PublishStream
 *
 * @author sidd0610
 */
public class PublishStream extends CommonContext {
    private final int TIMEOUT_SECONDS = 30; // Max time we'll wait to finish streaming

    ClientCallStreamObserver<PublishRequest> requestObserver = null;

    private ByteString lastPublishedReplayId;

    public PublishStream(ExampleConfigurations exampleConfigurations) {
        super(exampleConfigurations);
        setupTopicDetails(exampleConfigurations.getTopic(), true, true);
    }

    /**
     * Publishes specified number of events using the PublishStream RPC.
     *
     * @param numEventsToPublish
     * @return ByteString
     * @throws Exception
     */
    public void publishStream(int numEventsToPublish, Boolean singlePublishRequest) throws Exception {
        CountDownLatch finishLatch = new CountDownLatch(1);
        AtomicReference<CountDownLatch> finishLatchRef = new AtomicReference<>(finishLatch);
        final int numExpectedPublishResponses = singlePublishRequest ? 1 : numEventsToPublish;
        final List<PublishResponse> publishResponses = Lists.newArrayListWithExpectedSize(numExpectedPublishResponses);
        AtomicInteger failed = new AtomicInteger(0);
        StreamObserver<PublishResponse> pubObserver = getDefaultPublishStreamObserver(finishLatchRef,
                numExpectedPublishResponses, publishResponses, failed);

        // construct the stream
        requestObserver = (ClientCallStreamObserver<PublishRequest>) asyncStub.publishStream(pubObserver);

        if (singlePublishRequest == false) {
            // Publish each event in a separate batch
            for (int i = 0; i < numEventsToPublish; i++) {
                requestObserver.onNext(generatePublishRequest(i, singlePublishRequest));
            }
        } else {
            // Publish all events in one batch
            requestObserver.onNext(generatePublishRequest(numEventsToPublish, singlePublishRequest));
        }

        validatePublishResponse(finishLatch, numExpectedPublishResponses, publishResponses, failed, numEventsToPublish);
        requestObserver.onCompleted();
    }

    /**
     * Helper function to validate the PublishResponse received. Also prints the RPC id of the call.
     *
     * @param errorStatus
     * @param finishLatch
     * @param expectedResponseCount
     * @param publishResponses
     * @return
     * @throws Exception
     */
    private void validatePublishResponse(CountDownLatch finishLatch,
                                               int expectedResponseCount, List<PublishResponse> publishResponses, AtomicInteger failed, int expectedNumEventsPublished) throws Exception {
        String exceptionMsg;
        boolean failedPublish = false;
        if (!finishLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            failedPublish = true;
            exceptionMsg = "[ERROR] publishStream timed out after: " + TIMEOUT_SECONDS + "sec";
            logger.error(exceptionMsg);
        }

        if (expectedResponseCount != publishResponses.size()) {
            failedPublish = true;
            exceptionMsg = "[ERROR] PublishStream received: " + publishResponses.size() + " PublishResponses instead of expected "
                    + expectedResponseCount;
            logger.error(exceptionMsg);
        }

        if (failed.get() != 0) {
            failedPublish = true;
            exceptionMsg = "[ERROR] Failed to publish all events. " + failed + " failed out of "
                    + expectedNumEventsPublished;
            logger.error(exceptionMsg);
        }

        if (failedPublish) {
            throw new RuntimeException("Failed to publish events.");
        }
    }

    /**
     * Creates a ProducerEvent to be published in a PublishRequest.
     *
     * @param counter
     * @return
     * @throws IOException
     */
    private ProducerEvent generateProducerEvent(int counter) throws IOException {
        Schema schema = new Schema.Parser().parse(schemaInfo.getSchemaJson());
        GenericRecord event = createEventMessage(schema, counter);

        // Convert to byte array
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(event.getSchema());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(buffer, null);
        writer.write(event, encoder);

        return ProducerEvent.newBuilder().setSchemaId(schemaInfo.getSchemaId())
                .setPayload(ByteString.copyFrom(buffer.toByteArray())).build();
    }

    /**
     * Creates an array of ProducerEvents to be published in a PublishRequest.
     *
     * @param count
     * @return
     * @throws IOException
     */
    private ProducerEvent[] generateProducerEvents(int count) throws IOException {
        Schema schema = new Schema.Parser().parse(schemaInfo.getSchemaJson());
        List<GenericRecord> events = createEventMessages(schema, count);

        ProducerEvent[] prodEvents = new ProducerEvent[count];
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(events.get(0).getSchema());

        for(int i=0; i<count; i++) {
            // Convert to byte array
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(buffer, null);
            writer.write(events.get(i), encoder);
            // Setting event id which can be used to correlate PublishResponses returned from Pub/Sub API server
            prodEvents[i] = ProducerEvent.newBuilder().setSchemaId(schemaInfo.getSchemaId())
                    .setPayload(ByteString.copyFrom(buffer.toByteArray())).setId(UUID.randomUUID().toString()).build();
        }

        return prodEvents;
    }

    /**
     * Helper function to generate the PublishRequest with the generated ProducerEvent to be sent
     * using the PublishStream RPC
     *
     * @return PublishRequest
     * @throws IOException
     */
    private PublishRequest generatePublishRequest(int count, Boolean singlePublishRequest) throws IOException {
        if (singlePublishRequest == false) {
            // One event per batch
            ProducerEvent e = generateProducerEvent(count);
            return PublishRequest.newBuilder().setTopicName(busTopicName).addEvents(e).build();
        } else {
            // Multiple events per batch
            ProducerEvent[] prodEvents = generateProducerEvents(count);
            PublishRequest.Builder p = PublishRequest.newBuilder().setTopicName(busTopicName);
            for(int i=0; i<count; i++) {
                p.addEvents(prodEvents[i]);
            }
            return p.build();
        }
    }

    /**
     * Creates a StreamObserver for handling the incoming PublishResponse messages from the server.
     *
     * @param errorStatus
     * @param finishLatchRef
     * @param expectedResponseCount
     * @param publishResponses
     * @return
     */
    private StreamObserver<PublishResponse> getDefaultPublishStreamObserver(AtomicReference<CountDownLatch> finishLatchRef, int expectedResponseCount,
                                                                            List<PublishResponse> publishResponses, AtomicInteger failed) {
        return new StreamObserver<>() {
            @Override
            public void onNext(PublishResponse publishResponse) {
                publishResponses.add(publishResponse);

                logger.info("Publish Call rpcId: " + publishResponse.getRpcId());

                for (PublishResult publishResult : publishResponse.getResultsList()) {
                    if (publishResult.hasError()) {
                        failed.incrementAndGet();
                        logger.error("[ERROR] Publishing event with correlationKey: " + publishResult.getCorrelationKey() +
                                " failed with error: " + publishResult.getError().getMsg());
                    } else {
                        logger.info("Event published successful with correlationKey: " + publishResult.getCorrelationKey());
                        lastPublishedReplayId = publishResult.getReplayId();
                    }
                }
                if (publishResponses.size() == expectedResponseCount) {
                    finishLatchRef.get().countDown();
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.error("[ERROR] Unexpected error status: " + Status.fromThrowable(t));
                printStatusRuntimeException("Error during PublishStream", (Exception) t);
                finishLatchRef.get().countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("Successfully published events for topic " + busTopicName + " for tenant " + tenantGuid);
                finishLatchRef.get().countDown();
            }
        };
    }

    public static void main(String[] args) throws IOException {
        ExampleConfigurations exampleConfigurations = new ExampleConfigurations("arguments.yaml");

        // Using the try-with-resource statement. The CommonContext class implements AutoCloseable in
        // order to close the resources used.
        try (PublishStream example = new PublishStream(exampleConfigurations)) {
            example.publishStream(exampleConfigurations.getNumberOfEventsToPublish(),
                                  exampleConfigurations.getSinglePublishRequest());
        } catch (Exception e) {
            printStatusRuntimeException("Error During PublishStream", e);
        }
    }
}
