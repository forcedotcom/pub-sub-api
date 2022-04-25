# Getting Started with the Pub/Sub API

- [About](#about)
- [Resources](#resources)

## About

Welcome to the Pub/Sub API Pilot! This repo contains the critical [proto
file](https://github.com/developerforce/pub-sub-api-pilot/blob/main/pubsub_api.proto)
that you will need to use the API. Refer to the [pilot guide](https://resources.docs.salesforce.com/rel1/doc/en-us/static/pdf/Salesforce_Pub_Sub_API_Pilot.pdf) on how to use it. There is a Python example app in this repo (`python` directory) as well as multiple Go example apps (`go` directory); please read the corresponding READMEs carefully.

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
|Java|[Apache Avro Java](https://avro.apache.org/docs/1.10.2/gettingstartedjava.html)|
|Kotlin|[avro4k](https://github.com/avro-kotlin/avro4k)|
|Node|[avro-js](https://www.npmjs.com/package/avro-js)|
|Objective C|[ObjectiveAvro](https://github.com/jlawton/ObjectiveAvro) (but read [this](https://stackoverflow.com/questions/57216446/data-serialisation-in-objective-c-avro-alternative))|
|PHP|[avro-php](https://github.com/wikimedia/avro-php)|
|Python|[Apache Avro Python](https://avro.apache.org/docs/current/gettingstartedpython.html)|
|Ruby|[AvroTurf](https://github.com/dasch/avro_turf)|

## Resources

**Documentation**
- [Salesforce Developers Blog Post](https://developer.salesforce.com/blogs/2021/07/pub-sub-api-building-event-driven-integrations-just-got-even-easier)
- [Salesforce Developers Podcast Episode](https://developer.salesforce.com/podcast/2021/08/episode-93-pub-sub-api-with-emmett-chen-ran)

**Additional Code Samples**
- [Node client](github.com/pozil/pub-sub-api-node-client)
