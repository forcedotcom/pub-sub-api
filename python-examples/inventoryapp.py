from __future__ import print_function
from datetime import datetime, timedelta
import logging
import grpc
import time

from pubsub import PubSub
import eventbus_api_pb2 as pb2

myurl = 'https://ebapi.my.stmpb.stm.salesforce.com'
myusername = 'client@eb.api'
mypassword = 'test12345'
mypublishtopic = '/event/NewOrderConfirmation__e'

'''
Creates a PublishRequest per the proto file.
'''
def makePublishRequest(schemaid, recordid, obj):
    req = pb2.PublishRequest(
        topic_name = mypublishtopic,
        events = generateProducerEvents(schemaid, recordid, obj))
    return req

'''
Encodes the data to be sent in the event and creates a ProducerEvent per 
the proto file.
'''
def generateProducerEvents(schemaid, recordid, obj):
    schema = obj.getschemajson(schemaid)
    dt = datetime.now() + timedelta(days=5)
    payload = {
        "CreatedDate": int(datetime.now().timestamp()),
        "CreatedById": '005R0000000cw06IAA',
        "OpptyRecordId__c": recordid,
        "EstimatedDeliveryDate__c": int(dt.timestamp()),
        "Weight__c": 58.2}
    req = {
        "id": "234",
        "schema_id": schemaid,
        "payload": obj.encode(schema, payload),
    }
    return [req]

'''
Callback function to handle events that arrive.
'''
def processorder(event, obj):
    payloadbytes = event.events[0].event.payload
    schemaid = event.events[0].event.schema_id
    decoded = obj.decode(obj.getschemajson(schemaid), 
        payloadbytes)
    #print(decoded['ChangeEventHeader']['changedFields'])
    #import pdb; pdb.set_trace()

    # Do not process updates to EstimatedDeliveryDate__c field
    deliverydatefield = '0x01000000'
    if deliverydatefield in decoded['ChangeEventHeader']['changedFields']:
        return

    recordid = decoded['ChangeEventHeader']['recordIds'][0]
    schema = obj.getschemajson(schemaid)

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

    pubschemaid = obj.stub.GetTopic(pb2.TopicRequest(topic_name=mypublishtopic), 
        metadata=obj.metadata).schema_id
        
    # Publish NewOrderConfirmation__e event
    # TODO
    res = obj.stub.Publish(makePublishRequest(pubschemaid, recordid, obj),
        metadata=obj.metadata)

def run():

    creds = grpc.local_channel_credentials(grpc.LocalConnectionType.LOCAL_TCP)
    with grpc.secure_channel('api.eventbus.salesforce.com:7011', creds) as channel:

        cdcListener = PubSub(myurl, myusername, mypassword, channel)
        cdcListener.auth()

        # Subscribe to Opportunity CDC events
        cdcListener.subscribe('/data/OpportunityChangeEvent', processorder)

if __name__ == '__main__':
    logging.basicConfig()
    run()
