"""
InventoryApp.py

This is a subscriber client that listens for Change Data Capture events for the
Opportunity object and publishes `/event/NewOrderConfirmation__e` events. In
the example, this file would be hosted somewhere outside of Salesforce. The `if
__debug__` conditionals are to slow down the speed of the app for demoing
purposes.
"""
import os, sys, avro

dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)

from datetime import datetime, timedelta
import logging

from PubSub import PubSub
import pubsub_api_pb2 as pb2
from utils.ClientUtil import command_line_input
import time
from util.ChangeEventHeaderUtility import process_bitmap

my_publish_topic = '/event/NewOrderConfirmation__e'


def make_publish_request(schema_id, record_id, obj):
    """
    Creates a PublishRequest per the proto file.
    """
    req = pb2.PublishRequest(
        topic_name=my_publish_topic,
        events=generate_producer_events(schema_id, record_id, obj))
    return req


def generate_producer_events(schema_id, record_id, obj):
    """
    Encodes the data to be sent in the event and creates a ProducerEvent per
    the proto file.
    """
    schema = obj.get_schema_json(schema_id)
    dt = datetime.now() + timedelta(days=5)
    payload = {
        "CreatedDate": int(datetime.now().timestamp()),
        "CreatedById": '005R0000000cw06IAA',
        "OpptyRecordId__c": record_id,
        "EstimatedDeliveryDate__c": int(dt.timestamp()),
        "Weight__c": 58.2}
    req = {
        "schema_id": schema_id,
        "payload": obj.encode(schema, payload),
    }
    return [req]


def process_order(event, pubsub):
    """
    This is a callback that gets passed to the `PubSub.subscribe()` method. It
    decodes the payload of the received event and extracts the opportunity ID.
    Next, it calls a helper function to publish the
    `/event/NewOrderConfirmation__e` event. For simplicity, this sample uses an
    estimated delivery date of five days from the current date. When no events
    are received within a certain time period, the API's subscribe method sends
    keepalive messages and the latest replay ID through this callback.
    """
    if event.events:
        print("Number of events received in FetchResponse: ", len(event.events))
        # If all requested events are delivered, release the semaphore
        # so that a new FetchRequest gets sent by `PubSub.fetch_req_stream()`.
        if event.pending_num_requested == 0:
            pubsub.release_subscription_semaphore()

        for evt in event.events:
            payload_bytes = evt.event.payload
            schema_id = evt.event.schema_id
            json_schema = pubsub.get_schema_json(schema_id)
            decoded_event = pubsub.decode(pubsub.get_schema_json(schema_id),
                                    payload_bytes)

            print("Received event payload: \n", decoded_event)
            #  A change event contains the ChangeEventHeader field. Check if received event is a change event. 
            if 'ChangeEventHeader' in decoded_event:
                # Decode the bitmap fields contained within the ChangeEventHeader. For example, decode the 'changedFields' field.
                # An example to process bitmap in 'changedFields'
                changed_fields = decoded_event['ChangeEventHeader']['changedFields']
                converted_changed_fields = process_bitmap(avro.schema.parse(json_schema), changed_fields)
                print("Change Type: " + decoded_event['ChangeEventHeader']['changeType'])
                print("=========== Changed Fields =============")
                print(converted_changed_fields)
                print("=========================================")
                # Do not process updates made by the SalesforceListener app to the opportunity record delivery date 
                if decoded_event['ChangeEventHeader']['changeOrigin'].find('client=SalesforceListener') != -1:
                    print("Skipping change event because it is an update to the delivery date by SalesforceListener.")
                    return

            record_id = decoded_event['ChangeEventHeader']['recordIds'][0]

            if __debug__:
                time.sleep(10)
            print("> Received new order! Processing order...")
            if __debug__:
                time.sleep(4)
            print("  Done! Order replicated in inventory system.")
            if __debug__:
                time.sleep(2)
            print("> Calculating estimated delivery date...")
            if __debug__:
                time.sleep(2)
            print("  Done! Sending estimated delivery date back to Salesforce.")
            if __debug__:
                time.sleep(10)

            topic_info = pubsub.get_topic(topic_name=my_publish_topic)

            # Publish NewOrderConfirmation__e event
            res = pubsub.stub.Publish(make_publish_request(topic_info.schema_id, record_id, pubsub),
                                    metadata=pubsub.metadata)
            if res.results[0].replay_id:
                print("> Event published successfully.")
            else:
                print("> Failed publishing event.")
    else:
        print("[", time.strftime('%b %d, %Y %l:%M%p %Z'), "] The subscription is active.")

    # The replay_id is used to resubscribe after this position in the stream if the client disconnects.
    # Implement storage of replay for resubscribe!!!
    event.latest_replay_id


def run(argument_dict):
    cdc_listener = PubSub(argument_dict)
    cdc_listener.auth()

    # Subscribe to Opportunity CDC events
    cdc_listener.subscribe('/data/OpportunityChangeEvent', "LATEST", "", 1, process_order)


if __name__ == '__main__':
    argument_dict = command_line_input(sys.argv[1:])
    logging.basicConfig()
    run(argument_dict)
