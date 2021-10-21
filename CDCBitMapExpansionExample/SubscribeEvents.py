import os, sys, time, avro

dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)

from utils.ClientUtil import *
from ChangeEventHeader import process_bitmap
from PubSub import PubSub


latest_replay_id = None

def process_events(event, pubsub):
    if event.events:
        payload_bytes = event.events[0].event.payload
        json_schema = pubsub.get_schema_json(event.events[0].event.schema_id)
        decoded_event = pubsub.decode(json_schema, payload_bytes)
        print("Got events!", decoded_event)
        if 'ChangeEventHeader' in decoded_event:
            changed_fields = decoded_event['ChangeEventHeader']['changedFields']
            print("=========== Changed Fields =============")
            print(process_bitmap(avro.schema.parse(json_schema), changed_fields))
            print("=========================================")
    else:
        print("[", time.strftime('%b %d, %Y %l:%M%p %Z'), "] The subscription is active.")

    # The replay_id is used to resubscribe after this position in the stream if the client disconnects.
    latest_replay_id = event.latest_replay_id


def subscribe(argument_dict):
    pubsub = PubSub(argument_dict)
    pubsub.auth()

    topic = pubsub.get_topic(pubsub.topic_name)

    # Get Schema Id from the topic
    schema = pubsub.get_schema_json(topic.schema_id)
    print("Schema = ", schema)

    # Subscribe
    print("=============== Starting Subscription ===============")
    pubsub.subscribe(pubsub.topic_name, process_events)


if __name__ == "__main__":
    argument_dict = command_line_input(sys.argv[1:])
    subscribe(argument_dict)
