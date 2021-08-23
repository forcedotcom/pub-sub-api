"""
InventoryApp.py

This is a subscriber client that listens for Change Data Capture events for the
Opportunity object and publishes `/event/NewOrderConfirmation__e` events. In
the example, this file would be hosted somewhere outside of Salesforce. The `if
__debug__` conditionals are to slow down the speed of the app for demoing
purposes.
"""
import os, sys

dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)

from datetime import datetime, timedelta
import logging

from PubSub import PubSub
import pubsub_api_pb2 as pb2
from utils.ClientUtil import command_line_input
import time

my_publish_topic = '/event/NewOrderConfirmation__e'
latest_replay_id = None

"""
python3 InventoryApp.py --username client@eb.api --password XXXXXXX --url https://ebapi.my.stmpb.stm.salesforce.com  --tenantId core/ebapi/00DR0000000IYeEMAW 
--https true --grpcHost eventbusapi-core4.sfdc-ckzqgc.svc.sfdcfc.net --grpcPort 7443
"""


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
        "id": "234",
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
        payload_bytes = event.events[0].event.payload
        schema_id = event.events[0].event.schema_id
        decoded = pubsub.decode(pubsub.get_schema_json(schema_id),
                                payload_bytes)

        # Do not process updates to EstimatedDeliveryDate__c field
        delivery_date_field = '0x01000000'
        if delivery_date_field in decoded['ChangeEventHeader']['changedFields']:
            return

        record_id = decoded['ChangeEventHeader']['recordIds'][0]

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
    latest_replay_id = event.latest_replay_id


def run(argument_dict):
    cdc_listener = PubSub(argument_dict)
    cdc_listener.auth()

    # Subscribe to Opportunity CDC events
    cdc_listener.subscribe('/data/OpportunityChangeEvent', process_order)


if __name__ == '__main__':
    argument_dict = command_line_input(sys.argv[1:])
    logging.basicConfig()
    run(argument_dict)
