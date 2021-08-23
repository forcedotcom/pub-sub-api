"""
PubSub.py

This file defines the class `PubSub`, which contains common functionality for
both publisher and subscriber clients. 
"""

import io
import threading
import xml.etree.ElementTree as et
from datetime import datetime

import avro.io
import avro.schema
import certifi
import grpc
import requests

import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
from urllib.parse import urlparse
from utils.ClientUtil import load_properties

properties = load_properties("../resources/application.properties")

with open(certifi.where(), 'rb') as f:
    secure_channel_credentials = grpc.ssl_channel_credentials(f.read())


def get_argument(key, argument_dict):
    if key in argument_dict.keys():
        return argument_dict[key]
    else:
        return properties.get(key)


class PubSub(object):
    """
    Class with helpers to use the Salesforce Pub/Sub API.
    """
    semaphore = threading.Semaphore(1)

    def __init__(self, argument_dict):
        self.url = get_argument('instance_url', argument_dict)
        self.username = get_argument('username', argument_dict)
        self.password = get_argument('password', argument_dict)
        self.metadata = None
        if get_argument('https', argument_dict) == 'true':
            grpc_host = get_argument('grpcHost', argument_dict)
            grpc_port = get_argument('grpcPort', argument_dict)
            url = grpc_host + ":" + grpc_port
            channel = grpc.secure_channel(url, secure_channel_credentials)
        else:
            channel = grpc.insecure_channel('localhost:7011')
        self.stub = pb2_grpc.PubSubStub(channel)
        self.session_id = None
        self.pb2 = pb2
        self.tenant_id = get_argument('tenant_id', argument_dict)
        self.topic_name = get_argument('topic', argument_dict)

    def auth(self):
        """
        Sends a login request to the Salesforce SOAP API to retrieve a session
        token. The session token is bundled with other identifying information
        to create a tuple of metadata headers, which are needed for every RPC
        call.
        """
        url_suffix = '/services/Soap/u/43.0/'
        headers = {'content-type': 'text/xml', 'SOAPAction': 'Login'}
        xml = "<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' " + \
              "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' " + \
              "xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>" + \
              "<urn:login><urn:username><![CDATA[" + self.username + \
              "]]></urn:username><urn:password><![CDATA[" + self.password + \
              "]]></urn:password></urn:login></soapenv:Body></soapenv:Envelope>"
        res = requests.post(self.url + url_suffix, data=xml, headers=headers)
        res_xml = et.fromstring(res.content.decode('utf-8'))[0][0][0]

        try:
            url_parts = urlparse(res_xml[3].text)
            self.url = "{}://{}".format(url_parts.scheme, url_parts.netloc)
            self.session_id = res_xml[4].text
        except IndexError:
            print("An exception occurred. Check the response XML below:", 
            res.__dict__)

        # Set metadata headers
        self.metadata = (('x-sfdc-api-session-token', self.session_id),
                         ('x-sfdc-instance-url', self.url),
                         ('x-sfdc-tenant-id', self.tenant_id))

    def make_fetch_request(self, topic):
        """
        Creates a FetchRequest per the proto file.
        """
        return pb2.FetchRequest(
            topic_name=topic,
            replay_preset=pb2.LATEST,
            num_requested=1)

    def fetch_req_stream(self, topic):
        """
        Returns a FetchRequest stream for the Subscribe RPC.
        """
        while True:
            self.semaphore.acquire()
            yield self.make_fetch_request(topic)

    def encode(self, schema, payload):
        """
        Uses Avro and the event schema to encode a payload. The `encode()` and
        `decode()` methods are helper functions to serialize and deserialize
        the payloads of events that clients will publish and receive using
        Avro. If you develop an implementation with a language other than
        Python, you will need to find an Avro library in that language that
        helps you encode and decode with Avro. When publishing an event, the
        plaintext payload needs to be Avro-encoded with the event schema for
        the API to accept it. When receiving an event, the Avro-encoded payload
        needs to be Avro-decoded with the event schema for you to read it in
        plaintext. 
        """
        schema = avro.schema.parse(schema)
        buf = io.BytesIO()
        encoder = avro.io.BinaryEncoder(buf)
        writer = avro.io.DatumWriter(schema)
        writer.write(payload, encoder)
        return buf.getvalue()

    def decode(self, schema, payload):
        """
        Uses Avro and the event schema to decode a serialized payload. The
        `encode()` and `decode()` methods are helper functions to serialize and
        deserialize the payloads of events that clients will publish and
        receive using Avro. If you develop an implementation with a language
        other than Python, you will need to find an Avro library in that
        language that helps you encode and decode with Avro. When publishing an
        event, the plaintext payload needs to be Avro-encoded with the event
        schema for the API to accept it. When receiving an event, the
        Avro-encoded payload needs to be Avro-decoded with the event schema for
        you to read it in plaintext.
        """
        schema = avro.schema.parse(schema)
        buf = io.BytesIO(payload)
        decoder = avro.io.BinaryDecoder(buf)
        reader = avro.io.DatumReader(schema)
        ret = reader.read(decoder)
        return ret

    def get_topic(self, topic_name):
        return self.stub.GetTopic(pb2.TopicRequest(topic_name=topic_name),
                                  metadata=self.metadata)

    def get_schema_json(self, schema_id):
        """
        Uses GetSchema RPC to retrieve schema given a schema ID.
        """
        res = self.stub.GetSchema(pb2.SchemaRequest(schema_id=schema_id),
                                  metadata=self.metadata)
        return res.schema_json

    def generate_producer_events(self, schema, schema_id):
        """
        Encodes the data to be sent in the event and creates a ProducerEvent per
        the proto file.
        """
        payload = {
            "CreatedDate": int(datetime.now().timestamp()),
            "CreatedById": '005R0000000cw06IAA',  # Your user ID
            "textt__c": 'text ==t Hello World'
        }
        req = {
            "id": "261",  # This can be anything
            "schema_id": schema_id,
            "payload": self.encode(schema, payload)
        }
        return [req]

    def subscribe(self, topic, callback):
        """
        Calls the Subscribe RPC defined in the proto file and accepts a
        client-defined callback to handle any events that are returned by the
        API. It uses a semaphore to prevent the Python client from closing the
        connection prematurely (this is due to the way Python's GRPC library is
        designed and may not be necessary for other languages--Java, for
        example, does not need this). 
        """
        sub_stream = self.stub.Subscribe(self.fetch_req_stream(topic),
                                         metadata=self.metadata)
        for event in sub_stream:
            self.semaphore.release()
            callback(event, self)

    def publish(self, topic_name, schema, schema_id):
        """
        Publishes events to the specified Platform Event topic.
        """

        return self.stub.Publish(self.pb2.PublishRequest(
            topic_name=topic_name,
            events=self.generate_producer_events(schema,
                                                 schema_id)),
            metadata=self.metadata)
