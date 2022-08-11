# Pub/Sub API Examples - Utility Code


## ChangeEventHeaderUtility.py
Because the Pub/Sub API is a binary API, delivered events are formatted as raw
Avro binary and sometimes contain fields that are not plaintext-readable. This
manifests in Change Data Capture events, causing them to look different from
how they are delivered when subscribing via Streaming API. 

For example, a Change Data Capture  event received via Pub/Sub API might look
like this:

```
{'ChangeEventHeader': 
	{
		...
		'nulledFields': [],
		'diffFields': [],
		'changedFields': ['0x650004E0']
	},
 ...
}
```

In this example, `changedFields` is encoded as a bitmap string. This method is
more space efficient than using a list of field names. A bit set to 1 in the
`changedFields` bitmap value indicates that the field at the corresponding
position in the Avro schema was changed. More information about bitmap fields
can be found in [Event Deserialization Considerations](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/event-deserialization-considerations.html) in the Pub/Sub API documentation.

We have provided this example to demonstrate how bitmap values can be decoded
so that they are human-readable and you can process the event by using the
values in the bitmap fields. 
