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

import genericpubsub.Subscribe;
import io.grpc.stub.StreamObserver;
import utility.CommonContext;
import utility.ExampleConfigurations;

/**
 * AccountUpdater
 * A subscribe client that listens to the `/event/NewAccount__e` custom platform events and updates
 * the appropriate Account Record with an AccountNumber using the Salesforce REST API.
 *
 * Example:
 * ./run.sh accountupdateapp.AccountUpdater
 *
 * @author sidd0610
 */

public class AccountUpdater {

    protected static final Logger logger = LoggerFactory.getLogger(AccountUpdater.class.getClass());

    protected Subscribe subscriber;
    private ExampleConfigurations subscriberParams;

    private static final String SUBSCRIBER_TOPIC = "/event/NewAccount__e";

    public AccountUpdater(ExampleConfigurations requiredParams) {
        logger.info("Setting Up Subscriber");
        this.subscriberParams = setupSubscriberParameters(requiredParams, SUBSCRIBER_TOPIC, 100);
        this.subscriber = new Subscribe(subscriberParams, getAccountUpdaterResponseObserver());
    }

    /**
     * Custom StreamObserver for the AccountUpdater.
     *
     * @return StreamObserver<FetchResponse>
     */
    private StreamObserver<FetchResponse> getAccountUpdaterResponseObserver() {
        return new StreamObserver<FetchResponse>() {
            @Override
            public void onNext(FetchResponse fetchResponse) {
                for(ConsumerEvent ce: fetchResponse.getEventsList()) {
                    try {
                        Schema writerSchema = subscriber.getSchema(ce.getEvent().getSchemaId());
                        GenericRecord eventPayload = CommonContext.deserialize(writerSchema, ce.getEvent().getPayload());
                        subscriber.updateReceivedEvents(1);
                        String accountRecordId = eventPayload.get("AccountRecordId__c").toString();
                        updateAccountRecord(subscriberParams, accountRecordId, subscriber.getSessionToken(), logger);
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

    public void stopApp() {
        subscriber.close();
    }

    public static void main(String[] args) throws IOException {
        // For this example specifying only the required configurations in the arguments.yaml is enough.
        ExampleConfigurations requiredParameters = new ExampleConfigurations("arguments.yaml");
        try {
            AccountUpdater ac = new AccountUpdater(requiredParameters);
            ac.startApp();
            ac.stopApp();
        } catch (Exception e) {
            printStatusRuntimeException("Error during AccountUpdate", e);
        }
    }
}
