package genericpubsub;

import java.io.IOException;

import org.apache.avro.Schema;

import com.salesforce.eventbus.protobuf.SchemaInfo;
import com.salesforce.eventbus.protobuf.SchemaRequest;
import com.salesforce.eventbus.protobuf.TopicInfo;
import com.salesforce.eventbus.protobuf.TopicRequest;

import utility.CommonContext;
import utility.ExampleConfigurations;

/**
 * An example that retrieves the Schema of a single-topic.
 *
 * Example:
 * ./run.sh genericpubsub.GetSchema
 *
 * @author sidd0610
 */
public class GetSchema extends CommonContext {

    public GetSchema(final ExampleConfigurations options) {
        super(options);
    }

    private void getSchema(String topicName) {
        // Use the GetTopic RPC to get the topic info for the given topicName.
        // Used to retrieve the schema id in this example.
        TopicInfo topicInfo = blockingStub.getTopic(TopicRequest.newBuilder().setTopicName(topicName).build());
        logger.info("GetTopic Call RPC ID: " + topicInfo.getRpcId());

        topicInfo.getAllFields().entrySet().forEach(entry -> {
            logger.info(entry.getKey() + " : " + entry.getValue());
        });

        SchemaRequest schemaRequest = SchemaRequest.newBuilder().setSchemaId(topicInfo.getSchemaId()).build();

        // Use the GetSchema RPC to get the schema info of the topic.
        SchemaInfo schemaResponse = blockingStub.getSchema(schemaRequest);
        logger.info("GetSchema Call RPC ID: " + schemaResponse.getRpcId());
        Schema schema = new Schema.Parser().parse(schemaResponse.getSchemaJson());

        // Printing the topic schema
        logger.info("Schema of topic  " + topicName + ": " + schema.toString(true));
    }

    public static void main(String[] args) throws IOException {
        ExampleConfigurations exampleConfigurations = new ExampleConfigurations("arguments.yaml");

        // Using the try-with-resource statement. The CommonContext class implements AutoCloseable in
        // order to close the resources used.
        try (GetSchema example = new GetSchema(exampleConfigurations)) {
            example.getSchema(exampleConfigurations.getTopic());
        } catch (Exception e) {
            printStatusRuntimeException("Getting schema", e);
        }
    }

}
