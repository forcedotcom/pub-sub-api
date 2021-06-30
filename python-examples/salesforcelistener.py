from __future__ import print_function
from datetime import datetime
import json
import logging
import requests
import grpc
import time

from pubsub import PubSub

myurl = 'https://ebapi.my.stmpb.stm.salesforce.com'
myusername = 'client@eb.api'
mypassword = 'test12345'

'''
Callback function to handle confirmation events. This listens for
NewOrderConfirmation__e events and updates the relevant Opportunity record
in Salesforce.
'''
def processconf(event, obj):
    payloadbytes = event.events[0].event.payload
    decoded = obj.decode(obj.getschemajson(event.events[0].event.schema_id), 
        payloadbytes)
    #print(decoded)
    print("> Received order confirmation! Updating estimated delivery date...")
    if __debug__:
        time.sleep(2)
    day = datetime.fromtimestamp(decoded['EstimatedDeliveryDate__c']).strftime('%Y-%m-%d')
    res = requests.patch(myurl + "/services/data/v52.0/sobjects/Opportunity/" 
        + decoded['OpptyRecordId__c'], json.dumps({"EstimatedDeliveryDate__c": day}), 
        headers={"Authorization":"Bearer " + obj.sessionid,
        "Content-Type": "application/json"})
    print("  Done!")

def run():

    creds = grpc.local_channel_credentials(grpc.LocalConnectionType.LOCAL_TCP)
    with grpc.insecure_channel('api.eventbus.salesforce.com:7011') as channel:

        sfdcUpdater = PubSub(myurl, myusername, mypassword, channel)
        sfdcUpdater.auth()

        # Subscribe to /event/NewOrderConfirmation__e events
        sfdcUpdater.subscribe('/event/NewOrderConfirmation__e', processconf)


if __name__ == '__main__':
    logging.basicConfig()
    run()
