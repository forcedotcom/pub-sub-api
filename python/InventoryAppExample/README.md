# Pub/Sub API Example - Inventory App 

This example of the Pub/Sub API is meant to be a conceptual example only—the
code is not a template, not meant for copying and pasting, and not intended to
serve as anything other than a read-only learning resource. We encourage you to
read through the code and this README in order to understand the logic
underpinning the example, so that you can take the learnings and apply them to
your own implementations. Also note that the way this example is structured is
but one way to interact with the Pub/Sub API using Python. You are free to
mirror the structure in your own code, but it is far from the only way to
engage with the API. 

The example imagines a scenario in which salespeople closing opportunities in
Salesforce need an "Estimated Delivery Date" field filled in by an integration
between Salesforce and an external inventory app. When an opportunity is closed
in Salesforce, a Change Data Capture event gets published by Salesforce. This
event gets consumed by an inventory app (`InventoryApp.py`) hosted in an
external system like AWS, which sets off the inventory process for the order,
like packaging, shipping, etc. Once the inventory app has calculated the
estimated delivery date for the order, it sends that information back to
Salesforce in the payload of a `NewOrderConfirmation` event. On the Salesforce
side, a subscriber client (`SalesforceListener.py`) receives the
`NewOrderConfirmation` event and uses the date contained in the payload to
update the very opportunity that just closed with its estimated delivery date.
In this scenario, this enables the salesperson who closed the deal to report
the estimated delivery date to their customer right away—the integration acts
so quickly that the salesperson can see the estimated delivery date almost
instantaneously after they close the opportunity. 

A video demonstrating this app in action can be found on the
[TrailheaDX](https://www.salesforce.com/trailheadx) website. After
registering/logging in, go to [Product & Partner
Demos](https://www.salesforce.com/trailheadx) and click `Integrations &
Analytics` > `Platform APIs` to watch it.

The proto file for the API can be found [here](https://github.com/developerforce/pub-sub-api/blob/main/pubsub_api.proto).

This example uses Python features that require Python version 3.10 or later, such as the `match` statement. 

To build a working client example in Python please follow [the Python Quick Start Guide.](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-python-quick-start.html)
