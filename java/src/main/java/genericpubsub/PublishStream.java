package genericpubsub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
 * A single-topic publisher that creates CarMaintenance events and publishes them. This example
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
    public ByteString publishStream(int numEventsToPublish) throws Exception {
        CountDownLatch finishLatch = new CountDownLatch(1);
        AtomicReference<CountDownLatch> finishLatchRef = new AtomicReference<>(finishLatch);
        final List<Status> errorStatuses = Lists.newArrayList();
        final List<PublishResponse> publishResponses = Lists.newArrayListWithExpectedSize(numEventsToPublish);
        StreamObserver<PublishResponse> pubObserver = getDefaultPublishStreamObserver(errorStatuses, finishLatchRef,
                numEventsToPublish, publishResponses);

        // construct the stream
        requestObserver = (ClientCallStreamObserver<PublishRequest>) asyncStub.publishStream(pubObserver);

        for (int i = 0; i < numEventsToPublish; i++) {
            requestObserver.onNext(generatePublishRequest(i));
        }

        ByteString lastPublishedReplayId = validatePublishResponse(errorStatuses, finishLatch, numEventsToPublish,
                publishResponses);
        requestObserver.onCompleted();
        return lastPublishedReplayId;
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
    private ByteString validatePublishResponse(List<Status> errorStatus, CountDownLatch finishLatch,
                                               int expectedResponseCount, List<PublishResponse> publishResponses) throws Exception {
        String exceptionMsg;
        final long LATEST = -1;
        ByteString lastPublishedReplayId = getReplayIdFromLong(LATEST);
        if (!finishLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            exceptionMsg = "[ERROR] publishStream timed out after: " + TIMEOUT_SECONDS + "sec";
            logger.error(exceptionMsg);
        }

        boolean receivedAllResponses = true;
        if (expectedResponseCount != publishResponses.size()) {
            receivedAllResponses = false;
            exceptionMsg = "[ERROR] publishStream received: " + publishResponses.size() + " events instead of expected "
                    + expectedResponseCount;
            logger.error(exceptionMsg);

            errorStatus.stream().forEach(status -> {
                logger.error("[ERROR] unexpected error status: " + status);
            });
        }

        AtomicInteger failed = new AtomicInteger(0);
        for (PublishResponse publishResponse : publishResponses) {
            List<PublishResult> errorList = publishResponse.getResultsList().stream().filter(PublishResult::hasError).collect(Collectors.toList());
            if (errorList.size() > 0) {
                logger.error("[ERROR] publishing some event(s) in batch failed with rpcId: " + publishResponse.getRpcId());
                for (PublishResult publishResult : errorList) {
                    failed.incrementAndGet();
                    logger.error("[ERROR] publishing event failed with: " + publishResult.getError().getMsg());
                }
            }
            else {
                logger.info("PublishStream Call rpcId: " + publishResponse.getRpcId());
                lastPublishedReplayId = publishResponse.getResultsList().get(0).getReplayId();
            }
        }

        if (failed.get() == 0 && receivedAllResponses) {
            logger.info("Successfully published " + expectedResponseCount + " events at " + busTopicName + " for tenant "
                    + tenantGuid);
        } else {
            exceptionMsg = "[ERROR] Failed to publish all events. " + failed + " failed out of "
                    + expectedResponseCount;
            logger.error(exceptionMsg);
            throw new Exception(exceptionMsg);
        }

        return lastPublishedReplayId;
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
        GenericRecord event = createCarMaintenanceRecord(schema, counter);

        // Convert to byte array
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(event.getSchema());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(buffer, null);
        writer.write(event, encoder);

        return ProducerEvent.newBuilder().setSchemaId(schemaInfo.getSchemaId())
                .setPayload(ByteString.copyFrom(buffer.toByteArray())).build();
    }

    /**
     * Helper function to generate the PublishRequest with the generated ProducerEvent to be sent
     * using the PublishStream RPC
     *
     * @return PublishRequest
     * @throws IOException
     */
    private PublishRequest generatePublishRequest(int counter) throws IOException {
        ProducerEvent e = generateProducerEvent(counter);
        return PublishRequest.newBuilder().setTopicName(busTopicName).addEvents(e).build();
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
    private StreamObserver<PublishResponse> getDefaultPublishStreamObserver(List<Status> errorStatus,
                                                                            AtomicReference<CountDownLatch> finishLatchRef, int expectedResponseCount,
                                                                            List<PublishResponse> publishResponses) {
        return new StreamObserver<>() {
            @Override
            public void onNext(PublishResponse pr) {
                publishResponses.add(pr);
                if (publishResponses.size() == expectedResponseCount) {
                    finishLatchRef.get().countDown();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                errorStatus.add(Status.fromThrowable(throwable));
                finishLatchRef.get().countDown();
            }

            @Override
            public void onCompleted() {
                finishLatchRef.get().countDown();
            }
        };
    }

    public static void main(String[] args) {
        try {
            ExampleConfigurations exampleConfigurations = new ExampleConfigurations("arguments.yaml");
            PublishStream example = new PublishStream(exampleConfigurations);

            // Using the try-with-resource statement. The CommonContext class implements AutoCloseable in
            // order to close the resources used.
            try (example) {
                example.publishStream(exampleConfigurations.getNumberOfEventsToPublish());
            } catch (Exception e) {
                if (example.requestObserver != null) {
                    example.requestObserver.onError(e);
                }
                throw e;
            }
        } catch (Exception e) {
            printStatusRuntimeException("Publishing events", e);
            System.exit(1);
        }
    }
}