package utility;

import java.net.URL;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.util.Fields;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth2 client credentials flow used to obtain access tokens for Pub/Sub API.
 *
 * The access token returned by this flow can be either an opaque session token or a JWT-based
 * (orgJWT) token. The token type is determined by the External Client App configuration in the
 * Salesforce org. JWT-based access tokens are short-lived and must be refreshed periodically while
 * a subscription stays open; the {@link CommonContext#scheduleAuthRefresh()} helper drives that
 * refresh by minting a fresh token via {@link #loginWithAccessToken()} and sending it back to the
 * server over the bidirectional stream as an {@code auth_refresh}.
 */
public class OAuthClientCredSessionFlow {
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuthClientCredSessionFlow.class);

    private static final String OAUTH2_TOKEN_ENDPOINT = "/services/oauth2/token";
    private final HttpClient httpClient;
    private final String loginEndpoint;
    private final String tenantId;
    private final String clientId;
    private final String clientSecret;

    public OAuthClientCredSessionFlow(HttpClient httpClient, String loginEndpoint, String tenantId, String clientId,
                                      String clientSecret) {
        this.httpClient = httpClient;
        this.loginEndpoint = loginEndpoint;
        this.tenantId = tenantId;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    /**
     * Performs the OAuth2 client credentials exchange and returns gRPC call credentials populated
     * with the freshly minted access token and the instance URL returned by the token endpoint.
     */
    public APISessionCredentials loginWithAccessToken() {
        JSONObject responseJson = requestOAuth2Token(loginEndpoint);
        Object accessToken = responseJson.get("access_token");
        Object instanceUrl = responseJson.get("instance_url");
        if (accessToken == null || instanceUrl == null) {
            throw new RuntimeException(
                    "OAuth2 token response is missing access_token or instance_url: " + responseJson);
        }
        LOGGER.debug("created OAuth2 session token credentials for tenant {} from {}", tenantId, instanceUrl);
        return new APISessionCredentials(tenantId, instanceUrl.toString(), accessToken.toString());
    }

    /**
     * Makes an HTTP POST request to the OAuth2 token endpoint and returns the parsed JSON response.
     *
     * @param loginEndpoint The OAuth2 token endpoint base URL
     * @return JSONObject containing the OAuth2 token response
     * @throws RuntimeException if the request fails or the response cannot be parsed
     */
    private JSONObject requestOAuth2Token(String loginEndpoint) {
        try {
            URL endpoint = new URL(loginEndpoint + OAUTH2_TOKEN_ENDPOINT);
            Request post = httpClient.POST(endpoint.toURI());

            post.header("Content-Type", "application/x-www-form-urlencoded");
            Fields fields = new Fields();
            fields.add("grant_type", "client_credentials");
            fields.add("client_id", clientId);
            fields.add("client_secret", clientSecret);

            post.content(new FormContentProvider(fields));
            ContentResponse response = post.send();

            if (response.getStatus() != 200) {
                String errorMessage = parseErrorResponse(response.getContentAsString());
                throw new RuntimeException(
                        String.format("OAuth2 client credentials error: %d - %s", response.getStatus(), errorMessage));
            }
            return (JSONObject) new JSONParser().parse(response.getContentAsString());
        } catch (Exception e) {
            throw new RuntimeException("Failed to obtain OAuth2 token", e);
        }
    }

    private static String parseErrorResponse(String responseBody) {
        try {
            JSONObject errorResponse = (JSONObject) new JSONParser().parse(responseBody);

            String error = errorResponse.get("error") == null ? null : errorResponse.get("error").toString();
            String errorDescription = errorResponse.get("error_description") == null ? null
                    : errorResponse.get("error_description").toString();
            if (error != null && errorDescription != null) {
                return String.format("%s: %s", error, errorDescription);
            } else if (error != null) {
                return error;
            }
        } catch (Exception e) {
            LOGGER.debug("Could not parse error response as JSON", e);
        }
        return responseBody;
    }
}
