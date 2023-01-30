using System.Threading.Channels;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSubApi;
using SalesforceIngestor.App.Config;
using SalesforceIngestor.App.Models;
using SalesforceIngestor.App.Services;

namespace SalesforceIngestor.App.BackgroundServices;

/// <summary>
/// Background service that starts up a manager for each subscription
/// </summary>
public class SubscriptionManagerService : BackgroundService
{
    private readonly PubSub.PubSubClient _grpcClient;
    private readonly Subscriptions _subscriptions;
    private readonly SubscriptionSettings _subscriptionSettings;
    private readonly ChannelWriter<EventMessage> _writer;
    private readonly SalesforcePubSubApiSettings _settings;
    private readonly ILogger<SubscriptionManagerService> _logger;
    private readonly IReplayIdRepository _replayIdRepository;
    private readonly IServiceProvider _serviceProvider;
    private readonly IMemoryCache _memoryCache;

    public SubscriptionManagerService(IOptions<Subscriptions> subscriptions, IOptions<SubscriptionSettings> subscriptionSettings, 
        ChannelWriter<EventMessage> writer, PubSub.PubSubClient grpcClient, ILogger<SubscriptionManagerService> logger, IReplayIdRepository replayIdRepository, 
        IServiceProvider serviceProvider, IMemoryCache memoryCache)
    {
        _subscriptions = subscriptions?.Value ?? throw new ArgumentNullException(nameof(subscriptions));
        _subscriptionSettings = subscriptionSettings?.Value ?? throw new ArgumentNullException(nameof(subscriptionSettings));
        _writer = writer ?? throw new ArgumentNullException(nameof(writer));
        _grpcClient = grpcClient ?? throw new ArgumentNullException(nameof(grpcClient));
        
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _replayIdRepository = replayIdRepository ?? throw new ArgumentException(nameof(replayIdRepository));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _memoryCache = memoryCache ?? throw new ArgumentNullException(nameof(memoryCache));
    }
    
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogDebug("{Method} called on {Class}", nameof(ExecuteAsync), nameof(SubscriptionManagerService));
        var tasks = _subscriptions.Topics.Select(x =>
        {
            var manager = new TopicSubscriptionManager(x, _subscriptionSettings,  _writer, _serviceProvider.GetRequiredService<ILogger<TopicSubscriptionManager>>(), _grpcClient,  _replayIdRepository, _memoryCache);
            return manager.RunAsync(stoppingToken);
        });

        await Task.WhenAll(tasks);
        _writer.TryComplete(); // this will allows the MessageProcessorService to stop
        _logger.LogDebug("{Method} exiting on {Class}", nameof(ExecuteAsync), nameof(SubscriptionManagerService));
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("{Method} called on {Class}", nameof(StopAsync), nameof(SubscriptionManagerService));
        return base.StopAsync(cancellationToken);
    }
}