package accountupdateapp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.ProducerEvent;
import com.salesforce.eventbus.protobuf.SchemaInfo;

import utility.ExampleConfigurations;

/**
 * The AccountUpdateAppUtil class provides helper functions such as creating NewAccount records,
 * NewAccount ProducerEvents, updating AccountRecord using REST API required for running the examples
 * in the AccountUpdateApp.
 */
public class AccountUpdateAppUtil {

    private static String changeEventHeaderFieldName = "ChangeEventHeader";

    /**
     * Creates the record/payload of the NewAccount custom platform event.
     *
     * @param schema
     * @param accountRecordId
     * @return
     */
    protected static GenericRecord createNewAccountRecord(Schema schema, String accountRecordId) {
        return new GenericRecordBuilder(schema).set("CreatedDate", System.currentTimeMillis() / 1000)
                .set("CreatedById", "<User_Id>").set("AccountRecordId__c", accountRecordId).build();
    }

    /**
     * Creates the ProducerEvent of the NewAccount custom platform event.
     *
     * @param schema
     * @param schemaInfo
     * @param accountRecordId
     * @return
     * @throws IOException
     */
    protected static ProducerEvent createNewAccountProducerEvent(Schema schema, SchemaInfo schemaInfo, String accountRecordId) throws IOException {
        GenericRecord event = createNewAccountRecord(schema, accountRecordId);

        // Convert to byte array
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(event.getSchema());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(buffer, null);
        writer.write(event, encoder);

        return ProducerEvent.newBuilder().setSchemaId(schemaInfo.getSchemaId())
                .setPayload(ByteString.copyFrom(buffer.toByteArray())).build();

    }

    /**
     * Parses the Account CDC event received to obtain the recordIds of the Account.
     *
     * @param eventRecord
     * @return
     * @throws ParseException
     */
    protected static List<String> getRecordIdsOfAccountCDCEvent(GenericRecord eventRecord) throws ParseException {
        List<String> recordIds = new ArrayList<>();
        JSONParser parser = new JSONParser();
        JSONObject changeEventHeaderJson;
        JSONArray recordIdsJson = new JSONArray();
        try {
            changeEventHeaderJson = (JSONObject) parser.parse(eventRecord.get(changeEventHeaderFieldName).toString());
            if (changeEventHeaderJson.get("changeType").toString().equals("CREATE")) {
                recordIdsJson = (JSONArray) parser.parse(changeEventHeaderJson.get("recordIds").toString());
            } else {
                return recordIds;
            }
        } catch (Exception e) {
            throw e;
        }
        for(Object o : recordIdsJson) {
            recordIds.add(o.toString());
        }
        return recordIds;
    }

    /**
     * Updates the Account Object's AccountNumber field with a generated UUID using the Salesforce REST API.
     *
     * @param subParams
     * @param accountRecordId
     * @param token
     * @param logger
     * @throws Exception
     */
    public static void updateAccountRecord(ExampleConfigurations subParams, String accountRecordId, String token, Logger logger) throws Exception {
        HttpClient client = new HttpClient();
        client.start();

        String accountNumber = UUID.randomUUID().toString();

        Request req = client.POST(subParams.getLoginUrl()+"/services/data/v52.0/sobjects/Account/" + accountRecordId + "?_HttpMethod=PATCH");

        req.header("Authorization", "Bearer " + token);
        req.header("Content-Type", "application/json");
        req.content(new StringContentProvider("{\"AccountNumber\": \"" + accountNumber + "\"}", "utf-8"));

        ContentResponse response = req.send();
        int res = response.getStatus();

        if (res > 299) {
            logger.info("Unable to update Account Record.");
            logger.info(response.getContentAsString());
        } else {
            logger.info("Successfully updated Account Record. Updated AccountNumber: " + accountNumber);
        }
        client.stop();
    }
}
