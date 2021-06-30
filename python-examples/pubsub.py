from datetime import datetime, timedelta
import threading
import avro.schema
import avro.io
import json
import requests
import io
import xml.etree.ElementTree as et

import eventbus_api_pb2_grpc as pb2_grpc
import eventbus_api_pb2 as pb2

'''
Class with helpers to use the Salesforce Pub/Sub API.
'''
class PubSub(object):

    semaphore = threading.Semaphore(1)

    def __init__(self, url, username, password, channel):
        self.url = url
        self.username = username
        self.password = password
        self.metadata = None
        self.stub = pb2_grpc.EventBusAPIServiceStub(channel)
        self.sessionid = None

    '''
    Authenticates the client and returns a session token.
    '''
    def auth(self):
        urlsuffix = '/services/Soap/u/43.0/'
        headers = {'content-type': 'text/xml', 'SOAPAction': 'Wololo'}
        xml = "<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' " + \
                "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' " + \
                "xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>" + \
                "  <urn:login>    <urn:username>" + self.username + \
                "</urn:username>    <urn:password>" + self.password + \
                "</urn:password>  </urn:login></soapenv:Body></soapenv:Envelope>"
        res = requests.post(self.url+urlsuffix, data=xml, headers=headers, verify=False)
        resxml = et.fromstring(res.content.decode('utf-8'))[0][0][0]
        self.sessionid = resxml[4].text

        #TODO replace with org ID?
        tenantid = 'core/ebapi/00DR0000000IYeEMAW'
        orgid18c = resxml[6][8].text
        orgid = '00DR0000000IYeE'

        # Set metadata headers
        self.metadata = (('x-sfdc-api-session-token', self.sessionid),
                        ('x-sfdc-instance-url', self.url),
                        ('x-sfdc-tenant-id', tenantid))

    '''
    Creates a FetchRequest per the proto file.
    '''
    def makeFetchRequest(self, topic):
        return pb2.FetchRequest(
                topic_name = topic,
                replay_preset = pb2.ReplayPreset.LATEST,
                num_requested = 1,
                linger_ms = 0)

    '''
    Returns a FetchRequest stream for the Subscribe RPC.
    '''
    def fetchReqStream(self, topic):
        while True:
            self.semaphore.acquire()
            yield self.makeFetchRequest(topic)

    '''
    Uses Avro and the event schema to decode a serialized payload.
    '''
    def decode(self, schema, payload):
        schema = avro.schema.Parse(schema)
        buf = io.BytesIO(payload)
        decoder = avro.io.BinaryDecoder(buf)
        reader = avro.io.DatumReader(writer_schema=schema)
        ret = reader.read(decoder)
        return ret

    '''
    Uses Avro and the event schema to encode a payload.
    '''
    def encode(self, schema, payload):
        schema = avro.schema.Parse(schema)
        buf = io.BytesIO()
        encoder = avro.io.BinaryEncoder(buf)
        writer = avro.io.DatumWriter(writer_schema=schema)
        writer.write(payload, encoder)
        return buf.getvalue()

    '''
    Uses GetSchema RPC to retrieve schema given a schema ID.
    '''
    def getschemajson(self, schemaid):
        res = self.stub.GetSchema(pb2.SchemaRequest(schema_id=schemaid), 
            metadata=self.metadata)
        return res.schema_json

    '''
    Subscribes to a Change Data Capture or Platform Event topic. Feeds
    each event to a callback function.
    '''
    def subscribe(self, topic, callback):
        substream = self.stub.Subscribe(self.fetchReqStream(topic), 
            metadata=self.metadata)
        for event in substream:
            self.semaphore.release()
            callback(event, self)

    '''
    Publishes events to the specified Platform Event topic.
    '''
    def publish(self, topic, PubReqGenerator):
        self.stub.Publish(PubReqGenerator, metadata=self.metadata)
