package processchangeeventheader;

import static utility.CommonContext.*;
import static utility.EventParser.getFieldListFromBitmap;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.eventbus.protobuf.ConsumerEvent;
import com.salesforce.eventbus.protobuf.FetchResponse;

import genericpubsub.Subscribe;
import io.grpc.stub.StreamObserver;
import utility.CommonContext;
import utility.ExampleConfigurations;

/**
 * ProcessChangeEventHeader
 * A subscribe client that listens to the specified Change Data Capture topic and extracts the
 * Changed Fields from the event received. In this example it listens to the events corresponding
 * to the Opportunity object.
 *
 * Example:
 * ./run.sh processchangeeventheader.ProcessChangeEventHeader
 *
 * @author sidd0610
 */

public class ProcessChangeEventHeader {

    protected static final Logger logger = LoggerFactory.getLogger(ProcessChangeEventHeader.class.getClass());

    protected Subscribe subscriber;
    private ExampleConfigurations subscriberParams;

    public ProcessChangeEventHeader(ExampleConfigurations exampleParams) {
        logger.info("Setting Up Subscriber");
        this.subscriberParams = setupSubscriberParameters(exampleParams, exampleParams.getTopic(), 100);
        this.subscriber = new Subscribe(subscriberParams, getProcessChangeEventHeaderResponseObserver());
    }

    private StreamObserver<FetchResponse> getProcessChangeEventHeaderResponseObserver() {
        return new StreamObserver<FetchResponse>() {
            @Override
            public void onNext(FetchResponse fetchResponse) {
                for(ConsumerEvent ce: fetchResponse.getEventsList()) {
                    try {
                        Schema writerSchema = subscriber.getSchema(ce.getEvent().getSchemaId());
                        GenericRecord eventPayload = CommonContext.deserialize(writerSchema, ce.getEvent().getPayload());
                        subscriber.updateReceivedEvents(1);
                        logger.info("Received event with Payload: " + eventPayload.toString());
                        List<String> changedFields = getFieldListFromBitmap(writerSchema, (GenericData.Record) eventPayload.get("ChangeEventHeader"), "changedFields");
                        if (!changedFields.isEmpty()) {
                            logger.info("============================");
                            logger.info("       Changed Fields       ");
                            logger.info("============================");
                            for (String field : changedFields) {
                                logger.info(field);
                            }
                            logger.info("============================");
                        }
                        System.out.println();
                    } catch (Exception e) {
                        logger.info(e.toString());
                    }

                }
            }

            @Override
            public void onError(Throwable t) {
                printStatusRuntimeException("Error during SubscribeStream", (Exception) t);
                subscriber.isActive.set(false);
            }

            @Override
            public void onCompleted() {
                logger.info("Received requested number of events! Call completed by server.");
                subscriber.isActive.set(false);
            }
        };
    }

    // Helper function to start the app.
    public void startApp() throws InterruptedException {
        subscriber.startSubscription();
    }

    // Helper function to stop the app.
    public void stopApp() {
        subscriber.close();
    }

    public static void main(String[] args) throws IOException {
        ExampleConfigurations exampleParams = new ExampleConfigurations("arguments.yaml");
        try {
            ProcessChangeEventHeader processChangeEventHeaderExample = new ProcessChangeEventHeader(exampleParams);
            processChangeEventHeaderExample.startApp();
            processChangeEventHeaderExample.stopApp();
        } catch (Exception e) {
            printStatusRuntimeException("Error while processing Change events", e);
        }
    }
}
