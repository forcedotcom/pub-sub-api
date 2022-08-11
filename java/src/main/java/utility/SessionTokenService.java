package utility;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.URL;
import java.nio.ByteBuffer;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.ByteBufferContentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * The SessionTokenService class is used for logging into the org used by the customer using the
 * Salesforce SOAP API and retrieving the tenandId and session token which will be used to create
 * CallCredentials. It also has a static subclass that parses the LoginResponse.
 */
public class SessionTokenService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionTokenService.class);

    private static final String ENV_END = "</soapenv:Body></soapenv:Envelope>";
    private static final String ENV_START =
            "<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' "
                    + "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
                    + "xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>";

    // The enterprise SOAP API endpoint used for the login call
    private static final String SERVICES_SOAP_PARTNER_ENDPOINT = "/services/Soap/u/43.0/";

    // HttpClient is thread safe and meant to be shared; assume callers are managing its life cycle correctly
    private final HttpClient httpClient;

    public SessionTokenService(HttpClient httpClient) {
        if (httpClient == null) {
            throw new IllegalArgumentException("HTTP client cannot be null");
        }

        this.httpClient = httpClient;
    }

    /**
     * Function to login with the username/password of the client.
     *
     * @param loginEndpoint
     * @param user
     * @param pwd
     * @param useProvidedLoginUrl
     * @return
     * @throws Exception
     */
    public APISessionCredentials login(String loginEndpoint, String user, String pwd, boolean useProvidedLoginUrl) throws Exception {
        URL endpoint;
        endpoint = new URL(new URL(loginEndpoint), SERVICES_SOAP_PARTNER_ENDPOINT);
        LOGGER.trace("requesting session token from {}", endpoint);
        Request post = httpClient.POST(endpoint.toURI());
        post.content(new ByteBufferContentProvider("text/xml", ByteBuffer.wrap(soapXmlForLogin(user, pwd))));
        post.header("SOAPAction", "''");
        post.header("PrettyPrint", "Yes");
        ContentResponse response = post.send();
        LoginResponseParser parser = parse(response);

        final String token = parser.sessionId;
        if (token == null || parser.serverUrl == null) {
            throw new ConnectException(String.format("Unable to login: %s", parser.faultstring));
        }

        if (null == parser.organizationId) {
            throw new ConnectException(
                    String.format("Unable to login: organization id is not found in the response"));
        }

        String url;
        if (useProvidedLoginUrl) {
            url = loginEndpoint;
        } else {
            // Form url to this format: https://na44.stmfa.stm.salesforce.com
            URL soapEndpoint = new URL(parser.serverUrl);
            url = soapEndpoint.getProtocol() + "://" + soapEndpoint.getHost();
            // Adding port info for local app setup
            if (soapEndpoint.getPort() > -1) {
                url += ":" + soapEndpoint.getPort();
            }
        }

        LOGGER.debug("created session token credentials for {} from {}", parser.organizationId, url);
        return new APISessionCredentials(parser.organizationId, url, token);
    }

    /**
     * Function to login with the tenantId and session token of the client.
     *
     * @param loginEndpoint
     * @param accessToken
     * @param tenantId
     * @return
     */
    public APISessionCredentials loginWithAccessToken(String loginEndpoint, String accessToken, String tenantId) {
        return new APISessionCredentials(tenantId, loginEndpoint, accessToken);
    }

    private static class LoginResponseParser extends DefaultHandler {

        private String buffer;
        private String faultstring;

        private boolean reading = false;
        private String serverUrl;
        private String sessionId;
        private String organizationId;

        @Override
        public void characters(char[] ch, int start, int length) {
            if (reading) {
                buffer = new String(ch, start, length);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) {
            reading = false;
            switch (localName) {
                case "organizationId":
                    organizationId = buffer;
                    break;
                case "sessionId":
                    sessionId = buffer;
                    break;
                case "serverUrl":
                    serverUrl = buffer;
                    break;
                case "faultstring":
                    faultstring = buffer;
                    break;
                default:
            }
            buffer = null;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) {
            switch (localName) {
                case "sessionId":
                case "serverUrl":
                case "faultstring":
                case "organizationId":
                    reading = true;
                    break;
                default:
            }
        }
    }

    private static LoginResponseParser parse(ContentResponse response) throws Exception {
        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            spf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            spf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            spf.setNamespaceAware(true);
            SAXParser saxParser = spf.newSAXParser();

            LoginResponseParser parser = new LoginResponseParser();

            saxParser.parse(new ByteArrayInputStream(response.getContent()), parser);

            return parser;
        } catch (SAXException e) {
            throw new Exception(String.format("Unable to login: %s::%s", response.getStatus(), response.getReason()));
        }
    }

    private static byte[] soapXmlForLogin(String username, String password) throws UnsupportedEncodingException {
        return (ENV_START + "  <urn:login>" + "    <urn:username>" + username + "</urn:username>" + "    <urn:password>"
                + password + "</urn:password>" + "  </urn:login>" + ENV_END).getBytes("UTF-8");
    }
}
