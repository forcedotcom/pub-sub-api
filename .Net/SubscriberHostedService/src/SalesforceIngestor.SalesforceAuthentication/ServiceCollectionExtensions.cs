using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Polly;

namespace SalesforceIngestor.SalesforceAuthentication;

public static class ServiceCollectionExtensions
{
    private static TimeSpan[] _httpBackoff = {TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10)};
    
    public static IServiceCollection AddSalesforceAuthenticationDependencies(this IServiceCollection services, Action<Settings> configurer)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        
        services.Configure(configurer);
        services.ConfigureOptions<SettingsConfigurer>();
        services.AddMemoryCache();
        services.AddHttpClient<ISalesforceTokenClient, SalesforceTokenClient>((provider, client) =>
        {
            client.BaseAddress = provider.GetRequiredService<IOptions<Settings>>().Value.LoginUri;
        }).AddTransientHttpErrorPolicy(builder => builder.WaitAndRetryAsync(_httpBackoff));
        
        
        return services;
    }
}