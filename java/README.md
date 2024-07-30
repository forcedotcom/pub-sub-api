# Pub/Sub API Java Examples

## Overview
This directory contains some Java examples that can be used with the Pub/Sub API. These examples range from generic Publish, Subscribe, ManagedSubscribe (beta), processing CustomEventHeaders in change events, and a specific example of updating the Salesforce Account standard object. It is important to note that these examples are not performance tested nor are they production ready. They are meant to be used as a learning resource or a starting point to understand the flows of each of the Remote Procedure Calls (RPCs) of Pub/Sub API. There are some limitations to these examples as well mentioned below.

## Project Structure
In the `src/main` directory of the project, you will find several sub-directories as follows:
* `java/`: This directory contains the main source code for all the examples grouped into separate packages:
  * `accountupdateapp/`: This package contains the examples for updating an Account standard object with an AccountNumber.
  * `genericpubsub/`: This package contains the examples covering the general flows of all RPCs of Pub/Sub API. 
  * `processchangeeventheader/`: This package contains an example for extracting the changed fields from a bitmap value in a change event.
  * `utility`: This package contains a list of utility classes used across all the examples. 
* `proto/` - This directory contains the same `pubsub_api.proto` file found at the root of this repo. The plugin used to generate the sources requires for this proto file to be present in the `src` directory.  
* `resources/` - This directory contains a list of resources needed for running the examples.

## Running the Examples
### Prerequisites
1. Install [Java 11](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html), [Maven](https://maven.apache.org/install.html).
2. Clone this project.
3. Run `mvn clean install` from the `java` directory to build the project and generate required sources from the proto file.
4. The `arguments.yaml` file in the `src/main/resources` sub-directory contains a list of required and optional configurations needed to run the examples. The file contains detailed comments on how to set the configurations.
5. Get the username, password, and login URL of the Salesforce org you wish to use.
6. For the examples in `genericpubsub` package, a custom **_Order Event_** [platform event](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_define_ui.htm) has to be created in the Salesforce org. Ensure your `Order Event` platform event matches the following structure:
   - Standard Fields
       - Label: `Order Event`
       - Plural Label: `Order Events`
   - Custom Fields
       - `Order Number` (Text, 18)
       - `City` (Text, 50)
       - `Amount` (Number, (16,2))
7. For the examples in the `accountupdateapp` package, another custom **_NewAccount_** [platform event](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_define_ui.htm) has to be created in the Salesforce org. [More info here](src/main/java/accountupdateapp/README.md).

### Execution
1. Update the configurations in the `src/main/resources/arguments.yaml` file. The required configurations will apply to all the examples and the optional ones depends on which example is being executed. The configurations include:
   1. Required configurations:
       * `PUBSUB_HOST`: Specify the Pub/Sub API endpoint to be used.
       * `PUBSUB_PORT`: Specify the Pub/Sub API port to be used (usually 7443).
       * `LOGIN_URL`: Specify the login url of the Salesforce org being used to run the examples.
       * `USERNAME` & `PASSWORD`: For authentication via username and password, you will need to specify the username and password of the Salesforce org. 
       * `ACCESS_TOKEN` & `TENANT_ID`: For authentication via session token and tenant ID, you will need to specify the sessionToken and tenant ID of the Salesforce org.
       *  When using managed event subscriptions (beta), one of these configurations is required.
          * `MANAGED_SUB_DEVELOPER_NAME`: Specify the developer name of ManagedEventSubscription. This parameter is used in ManagedSubscribe.java.
          * `MANAGED_SUB_ID`: Specify the ID of the ManagedEventSubscription Tooling API record. This parameter is used in ManagedSubscribe.java.

   2. Optional Parameters:
       * `TOPIC`: Specify the topic for which you wish to publish/subscribe. 
       * `NUMBER_OF_EVENTS_TO_PUBLISH`: Specify the number of events to publish while using the PublishStream RPC.
       * `SINGLE_PUBLISH_REQUEST`: Specify if you want to publish the events in a single or multiple PublishRequests.
       * `NUMBER_OF_EVENTS_IN_FETCHREQUEST`: Specify the number of events that the Subscribe RPC requests from the server in each FetchRequest. The example fetches at most 5 events in each Subscribe request. If you pass in more than 5, it sends multiple Subscribe requests with at most 5 events requested in FetchRequest each. For more information about requesting events, see [Pull Subscription and Flow Control](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/flow-control.html) in the Pub/Sub API documentation.
       * `PROCESS_CHANGED_FIELDS`: Specify whether the Subscribe or ManagedSubscribe client should process the [ChangedFields](https://developer.salesforce.com/docs/atlas.en-us.apexref.meta/apexref/apex_class_eventbus_ChangeEventHeader.htm#apex_eventbus_ChangeEventHeader_changedfields) header in ChangeDataCapture (CDC) events.
       * `REPLAY_PRESET`: Specify the ReplayPreset for subscribe examples.
         * If a subscription has to be started using the CUSTOM replay preset, the `REPLAY_ID` parameter is mandatory. 
         * The `REPLAY_ID` is a byte array and must be specified in this format: `[<byte_values_separated_by_commas>]`. Please enter the values as is within the square brackets and without any quotes. 
         * Example: `[0, 1, 2, 3, 4, -5, 6, 7, -8]`
       
2. After setting up the configurations, any example can be executed using the `./run.sh` file available at the parent directory.
   * Format for running the examples: `./run.sh <package_name>.<example_class_name>`
   * Example: `./run.sh genericpubsub.PublishStream`

## Implementation
- This repo can be used as a reference point for clients looking to create a Java app to integrate with Pub/Sub API. Note that the project structure and the examples included in this repo are intended for demo purposes only and clients are free to implement their own Java apps in any way they see fit.
- The Generic Subscribe and ManagedSubscribe (beta) RPC examples create a long-lived subscription. After all requested events are received, Subscribe sends a new `FetchRequest` and ManagedSubscribe sends a new `ManagedFetchRequest` to keep the subscription alive and the client listening to new events.
- The Generic Subscribe and ManagedSubscribe (beta) RPC examples demonstrate a basic flow control strategy where a new `FetchRequest` or `ManagedFetchRequest` is sent only after the requested number of events in the previous requests are received. The ManagedSubscribe RPC example also shows how to commit a Replay ID by sending commit requests. Custom flow control strategies can be implemented as needed. More info on flow control available [here](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/flow-control.html).
- The Generic Subscribe RPC example demonstrates error handling. After an exception occurs, it attempts to resubscribe after the last received event by implementing Binary Exponential Backoff. The example processes events and sends the retry requests asynchronously. If the error is an invalid replay ID,  it tries to resubscribe since the earliest stored event in the event bus. See the `onError()` method in `Subscribe.java`.

# Limitations
1. No guarantees that streams will remain open with `PublishStream` examples - Pub/Sub API has idle timeouts and will close idle streams. If a stream is closed while running these examples, you will most likely need to stop and restart.
2. No support for republishing on error - If an error occurs while publishing the relevant examples will surface the error but will not attempt to republish the event.
3. No security guarantees - Teams using these examples for reference will need to do their own security audits to ensure the dependencies introduced here can be safely used.
4. No performance testing - These examples have not been perf tested.
