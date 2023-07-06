# Getting Started with the Pub/Sub API

- [About Pub/Sub API](#about-pubsub-api)
- [gRPC](#grpc)
- [Documentation and Blog Posts](#documentation-and-blog-post)
- [Code Samples](#code-samples)

## About Pub/Sub API
Welcome to Pub/Sub API! Pub/Sub API provides a single interface for publishing and subscribing to platform events, including real-time event monitoring events, and change data capture events. Based on [gRPC](https://grpc.io/docs/what-is-grpc/introduction/) and HTTP/2, Pub/Sub API enables efficient delivery of binary event messages in the Apache Avro format.

This repo contains the critical [proto
file](https://github.com/developerforce/pub-sub-api/blob/main/pubsub_api.proto) that you will need to use the API. 

## gRPC
gRPC [officially supports 11 languages](https://grpc.io/docs/languages/), but
there is unofficial community support in more. To encode and decode events, an
Avro library for your language of choice will be needed. See below for which
officially supported languages have well-supported Avro libraries:

|Supported gRPC Language|Avro Libraries|
|-----------------------|--------------|
|C# | [AvroConvert](https://github.com/AdrianStrugala/AvroConvert)<br />[Apache Avro C#](https://avro.apache.org/docs/current/api/csharp/html/index.html) (docs are not great)|
|C++|[Apache Avro C++](https://avro.apache.org/docs/current/api/cpp/html/index.html)|
|Dart|[avro-dart](https://github.com/sqs/avro-dart) (last updated 2012)|
|Go|[goavro](https://github.com/linkedin/goavro)|
|Java|[Apache Avro Java](https://avro.apache.org/docs/current/getting-started-java/)|
|Kotlin|[avro4k](https://github.com/avro-kotlin/avro4k)|
|Node|[avro-js](https://www.npmjs.com/package/avro-js)|
|Objective C|[ObjectiveAvro](https://github.com/jlawton/ObjectiveAvro) (but read [this](https://stackoverflow.com/questions/57216446/data-serialisation-in-objective-c-avro-alternative))|
|PHP|[avro-php](https://github.com/wikimedia/avro-php)|
|Python|[Apache Avro Python](https://avro.apache.org/docs/current/getting-started-python/)|
|Ruby|[AvroTurf](https://github.com/dasch/avro_turf)|

## Documentation, Blog Post and Videos
- [Pub/Sub API Developer Guide](https://developer.salesforce.com/docs/platform/pub-sub-api/overview)
- [Salesforce Architects Blog Post](https://medium.com/salesforce-architects/announcing-pub-sub-api-generally-available-3980c9eaf0b7)
- [Introducing the New gRPC-based Pub Sub API YouTube Developer Quick Takes](https://youtu.be/g9P87_loVVA)

## Salesforce Support Code Samples
- [Java Quick Start in the Developer Guide](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-java-quick-start.html)
- [Python Quick Start in the Developer Guide](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-python-quick-start.html)
- [Python Code Examples](python/)
- [Go Code Examples](go/)
- [Java Code Examples](java/)

## Community Created Code Samples
These examples are not offiically supported by Salesforce. Please use with your own discretion.
- [E-Bikes Sample Application](https://github.com/trailheadapps/ebikes-lwc)
- [Pub/Sub API Node Client](https://github.com/pozil/pub-sub-api-node-client)
- [.Net Code Examples](https://github.com/Meyce/pub-sub-api/tree/main/.Net)
