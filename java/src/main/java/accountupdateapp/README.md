# Account Update App

This example subscribes to change events corresponding to the creation of [Account](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_account.htm) records. Also, it updates a field in the created records using a custom platform event as a mediator between the two processes.

## Prerequisites:
1. The `Account` entity needs to be selected in order to generate change events whenever there is any action taken wrt Account objects. Steps to enable this:
    * Go to the `Setup Home` in your Salesforce org
    * Under the `Platform Tools` section, click on `Change Data Capture`
    * Search for the `Account` object and click on the right arrow in the middle of the screen to select the entity.
    * Click on the `Save` button to update the changes.
2. The `NewAccount` custom platform needs to be created with the following fields:
    - Platform Event Name
        - Label: `NewAccount`
        - Plural Label: `NewAccounts`
    - Custom Fields
        - `AccountRecordId` (Text, 20)
3. Only the required configurations need to be specified in the `arguments.yaml` file while running this example. You can specify the other optional configurations, but the optional configurations required for this example will be overwritten while running the examples.

## Flow Overview:
* User creates an `Account` standard object which triggers an `AccountChangeEvent` event.
  * When the user creates an `Account` object, this generates a change event on the /data/AccountChangeEvent topic.
* A listener listens to this `AccountChangeEvent` event and publishes a `NewAccount` custom platform event
  * The listener subscribes to the events on the `/data/AccountChangeEvent` topic and only in the case when a new `Account` is created, it will publish an event on the `/event/NewAccount__e` custom platform event topic with the recordId of the created `Account` object. 
* The updater listens to this `NewAccount` event and updates the `Account` object with a randomly generated `AccountNumber`.
  * The updater subscribes to the `/event/NewAccount__e` topic and when an event is received, it will update the appropriate `Account` object with a randomly generated `AccountNumber` using the Salesforce REST API.  

## Running the examples:
1. Run the `AccountUpdater` first by running the following command:
```
./run.sh accountupdateapp.AccountUpdater
```
2. Run the `AccountListener` next by running the following command:
```
./run.sh accountupdateapp.AccountListener
```

## Notes:
* Please use the `my domain` URL for your org for running these examples. You can find the my domain URL through Developer Console.
  * Open Developer Console
  * Click on the Debug menu and select Open Execute Anonymous Window.
  * Key in the following in the window: `System.debug(System.url.getOrgDomainUrl());` and execute the same.
  * Once done, in the Logs tab below open the logs recently executed code. 
  * In the logs, get the `my domain` URL from the USER_DEBUG event.  
* Subscribers in both the `AccountUpdater` and `AccountListener` subscribe with the ReplayPreset set to LATEST. Therefore, only events generated once the examples have started running will be processed.
* The `AccountUpdater` logs the `AccountNumber` that has been added to the `Account` record which can be used to verify if the update is correct.