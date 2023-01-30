using System.Threading.Channels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Polly;
using PubSubApi;
using SalesforceIngestor.App.BackgroundServices;
using SalesforceIngestor.App.Config;
using SalesforceIngestor.App.GrpcComponents;
using SalesforceIngestor.App.Models;
using SalesforceIngestor.App.Services;
using SalesforceIngestor.SalesforceAuthentication;

namespace SalesforceIngestor.App;

public static class ServiceCollectionExtensions
{
    private static TimeSpan[] _httpBackoff = {TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10)};
    
    internal static IServiceCollection AddSalesforceIngestorAppDependencies(this IServiceCollection services, IConfiguration config)
    {
        services.ConfigureOptions<SubscriptionsConfigurer>();
        services.ConfigureOptions<SalesforcePubSubApiSettingsConfigurer>();
        services.ConfigureOptions<SubscriptionSettingsConfigurer>();

        services.AddSingleton(Channel.CreateUnbounded<EventMessage>(new UnboundedChannelOptions() { SingleReader = true }));
        services.AddSingleton(provider => provider.GetRequiredService<Channel<EventMessage>>().Reader);
        services.AddSingleton(provider => provider.GetRequiredService<Channel<EventMessage>>().Writer);

        //register the the replay id repository as a singleton
        services.AddSingleton<IReplayIdRepository, ReplayIdService>();
        
        //register that singleton repository as a hosted service
        services.AddHostedService(provider =>
            provider.GetRequiredService<IReplayIdRepository>() as ReplayIdService ??
            throw new InvalidOperationException(
                $"singleton {nameof(IReplayIdRepository)} as {nameof(ReplayIdService)} was null"));
        
        //add the hosted service
        //services.AddHostedService<SalesforceSubscriberService>();
        services.AddHostedService<MessageProcessorService>();
        services.AddHostedService<SubscriptionManagerService>();

        services.AddSalesforceAuthenticationDependencies(settings =>
        {
            config.GetSection("salesforceAuthenticationSettings").Bind(settings);
        });

        services.AddTransient<LoggingInterceptor>();
        
        services.AddGrpcClient<PubSub.PubSubClient>((provider, options)  =>
        {
            options.Address = provider.GetRequiredService<IOptions<SalesforcePubSubApiSettings>>().Value.ApiUri;
            
            
        }).AddCallCredentials(async (context, metadata, provider) =>
        {
            var client = provider.GetRequiredService<ISalesforceTokenClient>();
            var tokenResponse = await client.GetTokenResponseAsync().ConfigureAwait(false);
            metadata.Add("accesstoken", tokenResponse.AccessToken!);
            metadata.Add("instanceurl", tokenResponse.InstanceUrl!);
            metadata.Add("tenantid", tokenResponse.TenantId!);
        }).AddTransientHttpErrorPolicy(builder => builder.WaitAndRetryAsync(_httpBackoff));

        return services;
    }
}