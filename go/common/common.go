package common

import (
	"time"

	"github.com/developerforce/pub-sub-api/go/proto"
)

var (
	// topic and subscription-related variables
	TopicName           = "/event/CarMaintenance__e"
	ReplayPreset        = proto.ReplayPreset_EARLIEST
	ReplayId     []byte = nil
	Appetite     int32  = 5

	// gRPC server variables
	GRPCEndpoint    = "api.pubsub.salesforce.com:7443"
	GRPCDialTimeout = 5 * time.Second
	GRPCCallTimeout = 5 * time.Second

	// OAuth header variables
	GrantType    = "password"
	ClientId     = "<CLIENT_ID>"
	ClientSecret = "<CLIENT_SECRET>"
	Username     = "<ORG_USERNAME>"
	Password     = "<ORG_PASSWORD>"

	// OAuth server variables
	OAuthEndpoint    = "<ORG_LOGIN_URL>"
	OAuthDialTimeout = 5 * time.Second
)
