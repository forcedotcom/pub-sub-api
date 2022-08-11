package processchangeeventheader;

import static utility.CommonContext.*;
import static utility.EventParser.getFieldListFromBitmap;

import java.io.IOException;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.eventbus.protobuf.ConsumerEvent;
import com.salesforce.eventbus.protobuf.FetchResponse;

import genericpubsub.SubscribeStream;
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
 * ./run.sh accountupdateapp.ProcessChangeEventHeader
 *
 * @author sidd0610
 */

public class ProcessChangeEventHeader implements AutoCloseable {

    protected static final Logger logger = LoggerFactory.getLogger(ProcessChangeEventHeader.class.getClass());

    protected SubscribeStream subscriber;
    private ExampleConfigurations subscriberParams;

    private static final String SUBSCRIBER_TOPIC = "/data/OpportunityChangeEvent";

    public ProcessChangeEventHeader(ExampleConfigurations requiredParams) {
        logger.info("Setting Up Subscriber");
        this.subscriberParams = setupSubscriberParameters(requiredParams, SUBSCRIBER_TOPIC);
        this.subscriber = new SubscribeStream(subscriberParams, getProcessChangeEventHeaderResponseObserver());
    }

    private StreamObserver<FetchResponse> getProcessChangeEventHeaderResponseObserver() {
        return new StreamObserver<FetchResponse>() {
            @Override
            public void onNext(FetchResponse fetchResponse) {
                for(ConsumerEvent ce: fetchResponse.getEventsList()) {
                    try {
                        GenericRecord eventPayload = CommonContext.deserialize(subscriber.getSchema(), ce.getEvent().getPayload());
                        subscriber.updateReceivedEvents(1);
                        logger.info("Received event with Payload: " + eventPayload.toString());
                        List<String> changedFields = getFieldListFromBitmap(subscriber.getSchema(), (GenericData.Record) eventPayload.get("ChangeEventHeader"), "changedFields");
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
                CommonContext.printStatusRuntimeException("Error during SubscribeStream", (Exception) t);
            }

            @Override
            public void onCompleted() {
                logger.info("Received requested number of events! Call completed by server.");
            }
        };
    }

    // Helper function to start the app.
    public void startApp() throws InterruptedException {
        subscriber.startSubscription();
        subscriber.waitForEvents();
    }

    // Helper function to stop the app.
    public void stopApp() {
        subscriber.close();
    }

    @Override
    public void close() {
        logger.info("Bye Bye!");
    }

    public static void main(String[] args) throws IOException {
        ExampleConfigurations requiredParameters = new ExampleConfigurations("arguments.yaml");
        try (ProcessChangeEventHeader processChangeEventHeaderExample = new ProcessChangeEventHeader(requiredParameters)) {
            processChangeEventHeaderExample.startApp();
            processChangeEventHeaderExample.stopApp();
        } catch (Exception e) {

        }
    }
}
