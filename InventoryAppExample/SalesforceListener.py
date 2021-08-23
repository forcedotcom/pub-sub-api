"""
SalesforceListener.py

This is a subscriber client that listens for `/event/NewOrderConfirmation__e`
events published by the inventory app (`InventoryApp.py`). The `if __debug__`
conditionals are to slow down the speed of the app for demoing purposes.
"""

import os, sys

dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)

from datetime import datetime
import json
import logging
import requests
import time

from PubSub import PubSub
from utils.ClientUtil import command_line_input

my_url = 'https://ebapi.my.stmpb.stm.salesforce.com'
latest_replay_id = None

"""
python3 SalesforceListener.py --username client@eb.api --password XXXXXX --url https://ebapi.my.stmpb.stm.salesforce.com  --tenantId core/ebapi/00DR0000000IYeEMAW 
--https true --grpcHost eventbusapi-core4.sfdc-ckzqgc.svc.sfdcfc.net --grpcPort 7443
"""


def process_confirmation(event, pubsub):
    """
    This is a callback that gets passed to the `PubSub.subscribe()` method. It
    decodes the payload of the received event and extracts the opportunity ID
    and estimated delivery date. Using those two pieces of information, it
    updates the relevant opportunity with its estimated delivery date using the
    REST API. When no events are received within a certain time period, the
    API's subscribe method sends keepalive messages and the latest replay ID
    through this callback.
    """
    if event.events:
        payload_bytes = event.events[0].event.payload
        decoded = pubsub.decode(pubsub.get_schema_json(event.events[0].event.schema_id),
                                payload_bytes)
        # print(decoded)
        print("> Received order confirmation! Updating estimated delivery date...")
        if __debug__:
            time.sleep(2)
        day = datetime.fromtimestamp(decoded['EstimatedDeliveryDate__c']).strftime('%Y-%m-%d')
        res = requests.patch(my_url + "/services/data/v52.0/sobjects/Opportunity/"
                             + decoded['OpptyRecordId__c'], json.dumps({"EstimatedDeliveryDate__c": day}),
                             headers={"Authorization": "Bearer " + pubsub.session_id,
                                      "Content-Type": "application/json"})
        print("  Done!", res)
    else:
        print("[", time.strftime('%b %d, %Y %l:%M%p %Z'), "] The subscription is active.")

    # The replay_id is used to resubscribe after this position in the stream if the client disconnects.
    latest_replay_id = event.latest_replay_id

def run(argument_dict):
    sfdc_updater = PubSub(argument_dict)
    sfdc_updater.auth()

    # Subscribe to /event/NewOrderConfirmation__e events
    sfdc_updater.subscribe('/event/NewOrderConfirmation__e', process_confirmation)


if __name__ == '__main__':
    argument_dict = command_line_input(sys.argv[1:])
    logging.basicConfig()
    run(argument_dict)
