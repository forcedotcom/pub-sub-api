using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;

namespace SalesforceIngestor.App;

class Program
{
    static async Task Main(string[] args)
    {
        var hostBuilder = CreateHostBuilder(args).UseConsoleLifetime();

        using (var host = hostBuilder.Build())
        {
            var logger = host.Services.GetService<ILogger<Program>>();
            
            try
            {
                await host.RunAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException e)
            {
                logger?.LogError(e, "An operation cancelled exception has occurred.  This may be due to the host/StopAsync not having enough time to finish");
            }
            catch (Exception e)
            {
                logger?.LogError(e, "An unexpected exception occurred while executing the hosts.  Please verify that the host is running");
            }
        }
    }


    private static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureServices((hostContext, services) =>
            {
                //set host options for additional time to allow finish
                services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(30));
                services.AddSalesforceIngestorAppDependencies(hostContext.Configuration);
            })
            .UseSerilog((hostContext, loggingBuilder) =>
            {
                loggingBuilder.ReadFrom.Configuration(hostContext.Configuration);
            });
}