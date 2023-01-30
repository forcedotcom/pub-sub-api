using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;

namespace SalesforceIngestor.App.GrpcComponents;

public class LoggingInterceptor : Interceptor
{
    private readonly ILogger<LoggingInterceptor> _logger;

    public LoggingInterceptor(ILogger<LoggingInterceptor> logger)
    {
        _logger = logger;
    }
    
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        _logger.LogDebug("Intercept: {MethodName}", nameof(AsyncUnaryCall));
        return base.AsyncUnaryCall(request, context, continuation);
    }

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        _logger.LogDebug("Intercept: {MethodName}", nameof(BlockingUnaryCall));
        return base.BlockingUnaryCall(request, context, continuation);
    }

    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        _logger.LogDebug("Intercept: {MethodName}", nameof(UnaryServerHandler));
        return base.UnaryServerHandler(request, context, continuation);
    }

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(ClientInterceptorContext<TRequest, TResponse> context,
        AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        _logger.LogDebug("Intercept: {MethodName}", nameof(AsyncClientStreamingCall));
        return base.AsyncClientStreamingCall(context, continuation);
    }

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(ClientInterceptorContext<TRequest, TResponse> context,
        AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        _logger.LogDebug("Intercept: {MethodName}", nameof(AsyncDuplexStreamingCall));
        return base.AsyncDuplexStreamingCall(context, continuation);
    }

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context, AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        _logger.LogDebug("Intercept: {MethodName}", nameof(AsyncServerStreamingCall));
        return base.AsyncServerStreamingCall(request, context, continuation);
    }

    public override Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest> requestStream, ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        _logger.LogDebug("Intercept: {MethodName}", nameof(ClientStreamingServerHandler));
        return base.ClientStreamingServerHandler(requestStream, context, continuation);
    }

    public override Task DuplexStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream, ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        _logger.LogDebug("Intercept: {MethodName}", nameof(DuplexStreamingServerHandler));
        return base.DuplexStreamingServerHandler(requestStream, responseStream, context, continuation);
    }

    public override Task ServerStreamingServerHandler<TRequest, TResponse>(TRequest request, IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context, ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        _logger.LogDebug("Intercept: {MethodName}", nameof(ServerStreamingServerHandler));
        return base.ServerStreamingServerHandler(request, responseStream, context, continuation);
    }
}