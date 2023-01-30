# Introduction 
This is a hosted service designed for handling multiple subscriptions to events for Salesforce via the Pub/Sub API.  It is meant to be long-running and contains some components for resiliency via Polly.

# Operation
Then application manages subscriptions to 1 or more Salesforce Topics.  The gPRC interactions are managed via .Net's gRPC client factory.  The application provides resiliency via Polly and the application is meant to provide stable, long-lived connection to Salesforce Topics for data synchronization.  The application has a library for managing access tokens with Salesforce and a simple service for handling replay id storeage to the file system.  All messages received via th response stream are put on a .Net channel and are handled by another background service for further processing.

# Getting it running
Assuming this will be run against a test org, there are two config vals that need to be specified.  The topics that need to be subscribed to the parameters necessary for authentication need to be set.  This app assumes they will be set in `appsettings.json` or in `secrets.json`(recommended) as they can be set without changing code in the repository.  The code assumes the usage of certificates for authentication to Salesforce.  Refer to the `SalesforceIngestor.SalesforceAuthentication` project for more details.  The application uses the user secret `sf-ingestor-poc`.  The secret can be initialized and values for authentication can be set within secrets.json file.  With the below set the library will manage getting/caching tokens for Salesforce.  By default tokens will be cached for 1hr.

## Authentication

```
{
    "salesforceAuthenticationSettings": {
        "privateKeyIsPasswordProtected": false,
        "privateKeyIsBase64Encoded": false,
        "privateKey": "<put private key here.  best to base64 encode.  If it is base 64 encoded make sure privateKeyIsBase64Encoded is true>",
        "privateKeyPassword": "<put private key password here.  If the PK is base 64 encoded then make sure privateKeyIsPasswordProtected is true>",
        "clientId": "<The client id for the connected app in Salesforce>",        
        "userName": "<The salesforce user to login as>"        
      }
}
```

*Storing the private key for certificate authentication in the appsettings.json file should be avoided.  If certificate auth/assertion is not used for authentication then  a different implementation could also be provided that works with username/password as opposed to certificate.*

## Topics

Application can manage connections to more than 1 subscription at a time.  To specify which topics to subscribe to add the following to either the `secrets.json`(recommended) or the appsettings.json file.  The config value is an array and each entry will be suscribed to.  

```
{
    "subscriptions": {
        "topics": ["/event/<put the Salesforce schema name here>"]        
    }
}

```

*This sample was specifically tested with Platform Events.  CDC should work as well, but hasn't been explicitly tested.#

Salesforce OAuth 2.0 JWT Bearer Flow: https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5

