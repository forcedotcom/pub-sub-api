package utility;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import org.yaml.snakeyaml.Yaml;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.ReplayPreset;

/**
 * The ExampleConfigurations class is used for setting up the configurations for running the examples.
 * The configurations can be read from a YAML file or created directly via an object. It also sets
 * default values when an optional configuration is not specified.
 */
public class ExampleConfigurations {
    private String username;
    private String password;
    private String loginUrl;
    private String tenantId;
    private String accessToken;
    private String pubsubHost;
    private Integer pubsubPort;
    private String topic;
    private Integer numberOfEventsToPublish;
    private Boolean singlePublishRequest;
    private Integer numberOfEventsToSubscribeInEachFetchRequest;
    private Boolean plaintextChannel;
    private Boolean providedLoginUrl;
    private ReplayPreset replayPreset;
    private ByteString replayId;
    private String managedSubscriptionId;
    private String developerName;

    public ExampleConfigurations() {
        this(null, null, null, null, null,
                null, null, null, 5, false, 5,
                false, false, ReplayPreset.LATEST, null, null, null);
    }
    public ExampleConfigurations(String filename) throws IOException {

        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream("src/main/resources/"+filename);
        HashMap<String, Object> obj = yaml.load(inputStream);

        // Reading Required Parameters
        this.loginUrl = obj.get("LOGIN_URL").toString();
        this.pubsubHost = obj.get("PUBSUB_HOST").toString();
        this.pubsubPort = Integer.parseInt(obj.get("PUBSUB_PORT").toString());

        // Reading Optional Parameters
        this.username = obj.get("USERNAME") == null ? null : obj.get("USERNAME").toString();
        this.password = obj.get("PASSWORD") == null ? null : obj.get("PASSWORD").toString();
        this.topic = obj.get("TOPIC") == null ? "/event/Order_Event__e" : obj.get("TOPIC").toString();
        this.tenantId = obj.get("TENANT_ID") == null ? null : obj.get("TENANT_ID").toString();
        this.accessToken = obj.get("ACCESS_TOKEN") == null ? null : obj.get("ACCESS_TOKEN").toString();
        this.numberOfEventsToPublish = obj.get("NUMBER_OF_EVENTS_TO_PUBLISH") == null ?
                5 : Integer.parseInt(obj.get("NUMBER_OF_EVENTS_TO_PUBLISH").toString());
        this.singlePublishRequest = obj.get("SINGLE_PUBLISH_REQUEST") == null ?
                false : Boolean.parseBoolean(obj.get("SINGLE_PUBLISH_REQUEST").toString());
        this.numberOfEventsToSubscribeInEachFetchRequest = obj.get("NUMBER_OF_EVENTS_IN_FETCHREQUEST") == null ?
                5 : Integer.parseInt(obj.get("NUMBER_OF_EVENTS_IN_FETCHREQUEST").toString());
        this.plaintextChannel = obj.get("USE_PLAINTEXT_CHANNEL") != null && Boolean.parseBoolean(obj.get("USE_PLAINTEXT_CHANNEL").toString());
        this.providedLoginUrl = obj.get("USE_PROVIDED_LOGIN_URL") != null && Boolean.parseBoolean(obj.get("USE_PROVIDED_LOGIN_URL").toString());

        if (obj.get("REPLAY_PRESET") != null) {
            if (obj.get("REPLAY_PRESET").toString().equals("EARLIEST")) {
                this.replayPreset = ReplayPreset.EARLIEST;
            } else if (obj.get("REPLAY_PRESET").toString().equals("CUSTOM")) {
                this.replayPreset = ReplayPreset.CUSTOM;
                this.replayId = getByteStringFromReplayIdInputString(obj.get("REPLAY_ID").toString());
            } else {
                this.replayPreset = ReplayPreset.LATEST;
            }
        } else {
            this.replayPreset = ReplayPreset.LATEST;
        }

        this.developerName = obj.get("DEVELOPER_NAME") == null ? null : obj.get("DEVELOPER_NAME").toString();
        this.managedSubscriptionId = obj.get("MANAGED_EVENT_ID") == null ? null : obj.get("MANAGED_EVENT_ID").toString();
    }

    public ExampleConfigurations(String username, String password, String loginUrl,
                                 String pubsubHost, int pubsubPort, String topic) {
        this(username, password, loginUrl, null, null, pubsubHost, pubsubPort, topic,
                5, false, Integer.MAX_VALUE, false, false, ReplayPreset.LATEST, null, null, null);
    }

    public ExampleConfigurations(String username, String password, String loginUrl, String tenantId, String accessToken,
                                 String pubsubHost, Integer pubsubPort, String topic, Integer numberOfEventsToPublish,Boolean singlePublishRequest,
                                 Integer numberOfEventsToSubscribeInEachFetchRequest, Boolean plaintextChannel, Boolean providedLoginUrl,
                                 ReplayPreset replayPreset, ByteString replayId, String devName, String managedSubId) {
        this.username = username;
        this.password = password;
        this.loginUrl = loginUrl;
        this.tenantId = tenantId;
        this.accessToken = accessToken;
        this.pubsubHost = pubsubHost;
        this.pubsubPort = pubsubPort;
        this.topic = topic;
        this.numberOfEventsToPublish = numberOfEventsToPublish;
        this.numberOfEventsToSubscribeInEachFetchRequest = numberOfEventsToSubscribeInEachFetchRequest;
        this.plaintextChannel = plaintextChannel;
        this.providedLoginUrl = providedLoginUrl;
        this.replayPreset = replayPreset;
        this.replayId = replayId;
        this.developerName = devName;
        this.managedSubscriptionId = managedSubId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getLoginUrl() {
        return loginUrl;
    }

    public void setLoginUrl(String loginUrl) {
        this.loginUrl = loginUrl;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getPubsubHost() {
        return pubsubHost;
    }

    public void setPubsubHost(String pubsubHost) {
        this.pubsubHost = pubsubHost;
    }

    public int getPubsubPort() {
        return pubsubPort;
    }

    public void setPubsubPort(int pubsubPort) {
        this.pubsubPort = pubsubPort;
    }

    public Integer getNumberOfEventsToPublish() {
        return numberOfEventsToPublish;
    }

    public void setNumberOfEventsToPublish(Integer numberOfEventsToPublish) {
        this.numberOfEventsToPublish = numberOfEventsToPublish;
    }

    public Boolean getSinglePublishRequest() {
        return singlePublishRequest;
    }

    public void setSinglePublishRequest(Boolean singlePublishRequest) {
        this.singlePublishRequest = singlePublishRequest;
    }

    public int getNumberOfEventsToSubscribeInEachFetchRequest() {
        return numberOfEventsToSubscribeInEachFetchRequest;
    }

    public void setNumberOfEventsToSubscribeInEachFetchRequest(int numberOfEventsToSubscribeInEachFetchRequest) {
        this.numberOfEventsToSubscribeInEachFetchRequest = numberOfEventsToSubscribeInEachFetchRequest;
    }

    public boolean usePlaintextChannel() {
        return plaintextChannel;
    }

    public void setPlaintextChannel(boolean plaintextChannel) {
        this.plaintextChannel = plaintextChannel;
    }

    public Boolean useProvidedLoginUrl() {
        return providedLoginUrl;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setProvidedLoginUrl(Boolean providedLoginUrl) {
        this.providedLoginUrl = providedLoginUrl;
    }

    public ReplayPreset getReplayPreset() {
        return replayPreset;
    }

    public void setReplayPreset(ReplayPreset replayPreset) {
        this.replayPreset = replayPreset;
    }

    public ByteString getReplayId() {
        return replayId;
    }

    public void setReplayId(ByteString replayId) {
        this.replayId = replayId;
    }

    public String getManagedSubscriptionId() {
        return managedSubscriptionId;
    }

    public void setManagedSubscriptionId(String managedSubscriptionId) {
        this.managedSubscriptionId = managedSubscriptionId;
    }

    public String getDeveloperName() {
        return developerName;
    }

    public void setDeveloperName(String developerName) {
        this.developerName = developerName;
    }


    /**
     * NOTE: replayIds are meant to be opaque (See docs: https://developer.salesforce.com/docs/platform/pub-sub-api/guide/intro.html)
     * and this is used for example purposes only. A long-lived subscription client will use the stored replay to
     * resubscribe on failure. The stored replay should be in bytes and not in any other form.
     */
    public ByteString getByteStringFromReplayIdInputString(String input) {
        ByteString replayId;
        String[] values = input.substring(1, input.length()-2).split(",");
        byte[] b = new byte[values.length];
        int i=0;
        for (String x : values) {
            if (x.strip().length() != 0) {
                b[i++] = (byte)Integer.parseInt(x.strip());
            }
        }
        replayId = ByteString.copyFrom(b);
        return replayId;
    }
}
