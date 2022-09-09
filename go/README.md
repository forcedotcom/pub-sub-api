# Purpose
This directory contains some simple Go examples that can be used with Pub/Sub API. The examples show the general flow for each of the remote procedure calls (RPCs) supported by Pub/Sub API. Note that the examples are provided as a learning resource to demonstrate how to use the RPC methods in the Go language. They weren't performance tested and aren't production ready. See the `Limitations` section below for more details.

# Project Structure
* `common/` - This directory contains a set of common variables that are shared between all examples. Before running any of the examples, manually update these variables to ensure you're passing the correct username, password, etc. See the `Running the Examples` section below for instructions on how to prepare this file.
* `examples/` - This directory contains standalone examples for each RPC method defined in the proto file. When running an example you will use one of the `main.go` files nested in this directory as your entry point. In general, each example creates a gRPC client, fetches the necessary authentication token, and then calls the necessary RPC method(s) to demonstrate usage.
* `grpcclient/` - This directory contains a wrapper struct that is used by the examples to interact with the gRPC server. The majority of the logic for these examples lives in this wrapper struct.
* `oauth/` - This directory contains basic helper functions to fetch an OAuth token and user data.
* `proto/` - This directory contains the Go-specific `*.pb.go` files that are generated from the `pubsub_api.proto` file found at the root of
this project. The `*.pb.go` files contain helper functions and structs that will be used by the Go examples to interact with Pub/Sub API.

# Running the Examples
## Prerequisites
1. Install [Go](https://go.dev/doc/install).
2. Clone this project.
3. Run `go mod vendor` from the `go` directory to fetch dependencies.
    - NOTE: At this time the `vendor` directory has not been committed to this repo so you need to manually run the `go mod vendor` command to fetch dependencies. If the `vendor` directory is committed to the repo in the future, this step can be skipped.
4. Use a Salesforce org. Get the username, password, and login URL.
5. Create a custom CarMaintenance [platform event](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_define_ui.htm) in your Salesforce org. Ensure your CarMaintenance platform event matches the following structure:
- Standard Fields
    - Label: CarMaintenance
    - Plural Label: CarMaintenances
- Custom Fields
    - Cost (Number)
    - Mileage (Number)
    - WorkDescription (Text, 200)
6. Create a connected app and enable OAuth in your Salesforce org. See [Quick Start Prerequisites](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/quickstart_prereq.htm) in the REST API Developer Guide. Save the consumer key and consumer secret.

## Execution
1. Update the relevant variables in the `common/common.go` file. These configs will apply to all examples.
    * `<CLIENT_ID>` - Fill this value in with the consumer key you received after setting up OAuth for your Salesforce org.
    * `<CLIENT_SECRET>` - Fill this value in with the consumer secret you received after setting up OAuth for your Salesforce org.
    * `<ORG_USERNAME>` - Fill this value in with the username for your Salesforce org.
    * `<ORG_PASSWORD>` - Fill this value in with the password for your Salesforce org.
    * `<ORG_LOGIN_URL>` - Fill this value in with the login URL for your Salesforce org.
2. Choose an example to run from the `examples` directory. Each subdirectory is named after the RPC call flow it demonstrates and you can run the corresponding example with a `go run` command. For example, if you want to run the example for the GetTopic RPC, run `go run examples/topic/main.go` from the `go` directory.

## Subscription Options and Configuration
Pub/Sub API allows clients using the `Subscribe` RPC to specify one of the following subscription replay options:
* `LATEST`
* `CUSTOM`
* `EARLIEST`

For a description of these replay options, refer to the "Replaying an Event Stream" section in the [Pub/Sub API docs](https://developer.salesforce.com/docs/platform/pub-sub-api/references/methods/subscribe-rpc.html#replaying-an-event-stream).

The subscribe example in this repo is compatible with all three options outlined above. To set a subscription option, update the `ReplayPreset` and `ReplayId` options in the `common.go` file. See the following examples:
* `LATEST`
    - set `ReplayPreset = proto.ReplayPreset_LATEST`
    - set `ReplayId []byte = nil`
* `CUSTOM`
    - set `ReplayPreset = proto.ReplayPreset_CUSTOM`
    - set `ReplayId = []byte{0, 0, 0, 0, 0, 37, 132, 136}`
        - Note that this is just an example ReplayId intended to show the appropriate formatting. You need to specify a valid ReplayId in the same format demonstrated here.
* `EARLIEST`
    - set `ReplayPreset = proto.ReplayPreset_EARLIEST`
    - set `ReplayId []byte = nil`

# Creating Your Own Project
## Prerequisites
1. Install protoc, protoc-gen-go, and protoc-gen-go-grpc and ensure the binaries are available in your PATH. See the [gRPC quick start docs](https://grpc.io/docs/languages/go/quickstart/#prerequisites) for more info.

## Generating the *.pb.go Files
1. Fetch the most current [proto file](https://github.com/developerforce/pub-sub-api/blob/main/pubsub_api.proto) and copy it to your own project.
2. Modify the `option go_package` line to match your desired import path.
3. Generate the Go files from the proto file with the `protoc` command. As an example, if you copied the proto file to `proto/pubsub_api.proto` then you can generate the `*.pb.go` files by running the following command from the root of your project:
```bash
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/pubsub_api.proto
```

## Implementation
This repo can be used as a reference point for clients looking to create a Go app to integrate with Pub/Sub API. Note that the project structure and the examples included in this repo are intended for demo purposes; clients are free to implement their own Go apps in any way they see fit. A client may want to consider whether or not the error handling, retries, etc. included in these examples will be appropriate for their specific use case.

# Limitations
1. No support for auth token refreshes - At some point the authentication token will expire. At this time, these examples do not handle re-authentication.
2. No guarantees that streams will remain open - Pub/Sub API has idle timeouts and will close idle streams. If a stream is closed while running these examples, you will most likely need to stop and restart.
    * NOTE: This point primarily applies to the `Subscribe` and `PublishStream` RPC examples as these RPCs rely on long-lived streams.
3. No support for republishing on error - If an error occurs while publishing the relevant examples will surface the error but will not attempt to republish the event.
4. No security guarantees - Teams using these examples for reference will need to do their own security audits to ensure the dependencies introduced here can be safely used.
5. No performance testing - These examples have not been perf tested.
6. No timeouts for streaming calls - The `Subscribe` and `PublishStream` RPC examples do not have timeouts configured.