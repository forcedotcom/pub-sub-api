package utility;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.*;

public class XClientTraceIdClientInterceptor implements ClientInterceptor {
    private static final Logger logger = LoggerFactory.getLogger(XClientTraceIdClientInterceptor.class.getClass());
    private static final Metadata.Key<String> X_CLIENT_TRACE_ID = Metadata.Key.of("x-client-trace-id", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                String xClientTraceId = UUID.randomUUID().toString();
                headers.put(X_CLIENT_TRACE_ID, xClientTraceId);
                logger.info("sending request for xClientTraceId {}", xClientTraceId);

                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        logger.info("request completed for xClientTraceId {} with status {}", xClientTraceId, status);
                        super.onClose(status, trailers);
                    }
                }, headers);
            }
        };
    }
}
