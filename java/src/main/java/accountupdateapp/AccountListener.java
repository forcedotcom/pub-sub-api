package accountupdateapp;

import static accountupdateapp.AccountUpdateAppUtil.*;
import static utility.CommonContext.*;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.eventbus.protobuf.ConsumerEvent;
import com.salesforce.eventbus.protobuf.FetchResponse;

import genericpubsub.Publish;
import genericpubsub.Subscribe;
import io.grpc.stub.StreamObserver;
import utility.CommonContext;
import utility.ExampleConfigurations;

/**
 * AccountListener
 * A subscribe client that listens to the Change Data Capture (CDC) events of the Account object
 * and publishes events of the `/event/NewAccount__e` custom platform event.
 *
 * Example:
 * ./run.sh accountupdateapp.AccountListener
 *
 * @author sidd0610
 * @since v1.0
 */

public class AccountListener {

    protected static final Logger logger = LoggerFactory.getLogger(AccountListener.class.getClass());

    protected Subscribe subscriber;
    protected Publish publisher;

    private static final String SUBSCRIBER_TOPIC = "/data/AccountChangeEvent";
    private static final String PUBLISHER_TOPIC = "/event/NewAccount__e";

    public AccountListener(ExampleConfigurations requiredParams) {
        logger.info("Setting up the Subscriber");
        ExampleConfigurations subscriberParams = setupSubscriberParameters(requiredParams, SUBSCRIBER_TOPIC, 100);
        this.subscriber = new Subscribe(subscriberParams, getAccountListenerResponseObserver());
        logger.info("Setting up the Publisher");
        ExampleConfigurations publisherParams = setupPublisherParameters(requiredParams, PUBLISHER_TOPIC);
        this.publisher = new Publish(publisherParams);
    }

    /**
     * Custom StreamObserver for the AccountListener.
     *
     * @return StreamObserver<FetchResponse>
     */
    private StreamObserver<FetchResponse> getAccountListenerResponseObserver() {
        return new StreamObserver<FetchResponse>() {
            @Override
            public void onNext(FetchResponse fetchResponse) {
                for(ConsumerEvent ce: fetchResponse.getEventsList()) {
                    try {
                        Schema writerSchema = subscriber.getSchema(ce.getEvent().getSchemaId());
                        GenericRecord eventPayload = CommonContext.deserialize(writerSchema, ce.getEvent().getPayload());
                        subscriber.updateReceivedEvents(1);
                        for (String recordId : getRecordIdsOfAccountCDCEvent(eventPayload)) {
                            logger.info("New Account was Created");
                            publisher.publish(createNewAccountProducerEvent(publisher.getSchema(), publisher.getSchemaInfo(), recordId));
                        }
                    } catch (Exception e) {
                        logger.info(e.toString());
                    }
                }
                if (subscriber.getReceivedEvents().get() < subscriber.getTotalEventsRequested()) {
                    subscriber.fetchMore(subscriber.getBatchSize());
                } else {
                    subscriber.receivedAllEvents.set(true);
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
        subscriber.waitForEvents();
    }

    // Helper function to stop the app.
    public void stopApp() {
        subscriber.close();
        publisher.close();
    }

    public static void main(String[] args) throws IOException {
        // For this example specifying only the required configurations in the arguments.yaml is enough.
        ExampleConfigurations requiredParameters = new ExampleConfigurations("arguments.yaml");
        try {
            AccountListener ac = new AccountListener(requiredParameters);
            ac.startApp();
            ac.stopApp();
        } catch (Exception e) {
            printStatusRuntimeException("Error during AccountListener", e);
        }
    }
}
