package utility;

import java.util.UUID;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.CallCredentials;
import io.grpc.Metadata;

/**
 * The APISessionCredentials class extends the CallCredentials class of gRPC to add important
 * credential information, i.e., tenantId, accessToken and instanceUrl to every request made to
 * Pub/Sub API.
 */
public class APISessionCredentials extends CallCredentials {

    // Instance url of the customer org
    public static final Metadata.Key<String> INSTANCE_URL_KEY = keyOf("instanceUrl");
    // Session token of the customer
    public static final Metadata.Key<String> SESSION_TOKEN_KEY = keyOf("accessToken");
    // Tenant Id of the customer org
    public static final Metadata.Key<String> TENANT_ID_KEY = keyOf("tenantId");

    private String instanceURL;
    private String tenantId;
    private String token;

    private static final Logger log = LoggerFactory.getLogger(APISessionCredentials.class);

    public APISessionCredentials(String tenantId, String instanceURL, String token) {
        this.instanceURL = instanceURL;
        this.tenantId = tenantId;
        this.token = token;
    }

    @Override
    public void applyRequestMetadata(RequestInfo requestInfo, Executor executor, MetadataApplier metadataApplier) {
        log.debug("API session credentials applied to " + requestInfo.getMethodDescriptor());
        Metadata headers = new Metadata();
        headers.put(INSTANCE_URL_KEY, instanceURL);
        headers.put(TENANT_ID_KEY, tenantId);
        headers.put(SESSION_TOKEN_KEY, token);
        metadataApplier.apply(headers);
    }

    @Override
    public void thisUsesUnstableApi() {

    }

    private static Metadata.Key<String> keyOf(String name) {
        return Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
    }

    public String getToken() {
        return token;
    }
}