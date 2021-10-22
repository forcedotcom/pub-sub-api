# Pub/Sub API Example - Change Data Capture Bitmap Expansion

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
can be found under "Bitmap Fields in Change Events" in the [onboarding
guide](https://resources.docs.salesforce.com/rel1/doc/en-us/static/pdf/Salesforce_Pub_Sub_API_Pilot.pdf).

We have provided this example to demonstrate how bitmap values can be decoded
so that they are human-readable and you can process the event by using the
values in the bitmap fields. 
