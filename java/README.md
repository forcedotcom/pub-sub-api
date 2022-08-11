# Pub/Sub API Java Examples

## Overview
This directory contains some simple Java Examples that can be used with the Pub/Sub API. These examples range from generic Publish and Subscribe, processing CustomEventHeaders in Change Data Capture (CDC) events and also a specific example of updating the Salesforce Account standard object. It is important to note that these examples are not performance tested nor are they production ready. They are meant to be used as a learning resource or a starting point to understand the flows of each of the Remote Procedure Calls (RPCs) of Pub/Sub API. There are some limitations to these examples as well mentioned below.

## Project Structure
In the `src/main` directory of the project, you will find several sub-directories as follows:
* `java/`: This directory contains the main source code for all the examples grouped into separate packages:
  * `accountupdateapp/`: This package contains the examples for updating an Account standard object with an AccountNumber
  * `genericpubsub/`: This package contains the examples covering the general flows of all RPCs of Pub/Sub API. 
  * `processchangeeventheader/`: This package contains an example for extracting the changed fields from a bitmap value in a CDC event.
  * `utility`: This package contains a list of utility classes used across all the examples. 
* `proto/` - This directory contains the same `pubsub_api.proto` file found at the root of this repo. The plugin used to generate the sources requires for this proto file to be present in the `src` directory.  
* `resources/` - This directory contains a list of resources needed for running the examples.

## Running the Examples
### Prerequisites
1. Install [Java 11](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html), [Maven](https://maven.apache.org/install.html).
2. Clone this project.
3. Run `maven clean install` from the `java` directory to build the project and generate required sources from the proto file.
4. The `arguments.yaml` file in the `src/main/resources` sub-directory contains a list of required and optional configurations needed to run the examples. The file contains detailed comments on how to set the configurations.
5. Get the username, password, and login URL of the Salesforce org you wish to use.
6. For the examples in `genericpubsub` package, a custom **_CarMaintenance_** [platform event](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_define_ui.htm) has to be created in the Salesforce org. Ensure your CarMaintenance platform event matches the following structure:
   - Standard Fields
       - Label: `CarMaintenance`
       - Plural Label: `CarMaintenances`
   - Custom Fields
       - `Cost` (Number)
       - `Mileage` (Number)
       - `WorkDescription` (Text, 200)
7. For the examples in the `accountupdateapp` package, another custom **_NewAccount_** [platform event](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_define_ui.htm) has to be created in the Salesforce org. Ensure your NewAccount platform event matches the following structure:
   - Standard Fields
       - Label: `NewAccount`
       - Plural Label: `NewAccounts`
   - Custom Fields
       - `AccountRecordId` (Text, 20)

### Execution
1. Update the configurations in the `src/main/resources/arguments.yaml` file. The required configurations will apply to all the examples and the optional ones depends on which example is being executed. The configurations include:
   1. Required configurations:
       * `PUBSUB_HOST`: Specify the Pub/Sub API endpoint to be used.
       * `PUBSUB_PORT`: Specify the Pub/Sub API port to be used (usually 7443).
       * `LOGIN_URL`: Specify the login url of the Salesforce org being used to run the examples.
       * `USERNAME` & `PASSWORD`: For authentication via username and password, you will need to specify the username and password of the Salesforce org. 
       * `ACCESS_TOKEN` & `TENANT_ID`: For authentication via session token an tenant ID, you will need to specify the sessionToken and tenant ID of the Salesforce org.
   2. Optional Parameters:
       * `TOPIC`: Specify the topic for which you wish to publish/subscribe. 
       * `NUMBER_OF_EVENTS_TO_PUBLISH`: Specify the number of events to publish while using the PublishStream RPC.
       * `NUMBER_OF_EVENTS_TO_SUBSCRIBE`: Specify the number of events to subscribe while using the SubscribeStream RPC.
       * `USE_PLAINTEXT_CHANNEL`: Specify whether a plaintext channel has to be used.
       * `USE_PROVIDED_LOGIN_URL`: The LOGIN_URL required parameter is usually translated into a specific my domain URL for connecting to the Salesforce org. In case the supplied LOGIN_URL has to be used, set this configuration to `true`.
       * `REPLAY_PRESET`: Specify the ReplayPreset for subscribe examples.
2. After setting up the configurations, any example can be executed using the `./run.sh` file available at the parent directory.
   * Format for running the examples: `./run.sh <package_name>.<example_class_name>`
   * Example: `./run.sh genericpubsub.PublishStream`

## Implementation
This repo can be used as a reference point for clients looking to create a Java app to integrate with Pub/Sub API. Note that the project structure and the examples included in this repo are intended for demo purposes only and clients are free to implement their own Java apps in any way they see fit. 

# Limitations
1. No support for C2C type of authentication. This may be included in future updates.
2. No support for auth token refreshes - At some point the authentication token will expire. At this time, these examples do not handle re-authentication.
3. No guarantees that streams will remain open with `PublishStream` examples - Pub/Sub API has idle timeouts and will close idle streams. If a stream is closed while running these examples, you will most likely need to stop and restart.
4. No support for republishing on error - If an error occurs while publishing the relevant examples will surface the error but will not attempt to republish the event.
5. No security guarantees - Teams using these examples for reference will need to do their own security audits to ensure the dependencies introduced here can be safely used.
6. No performance testing - These examples have not been perf tested.