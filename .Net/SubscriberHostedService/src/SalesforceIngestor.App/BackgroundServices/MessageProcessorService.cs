using System.Buffers.Binary;
using System.Threading.Channels;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PubSubApi;
using SalesforceIngestor.App.Models;
using SalesforceIngestor.App.Services;
using SolTechnology.Avro;

namespace SalesforceIngestor.App.BackgroundServices;

/// <summary>
/// background service to further process the messages received.  Receives messages via a channel.
/// </summary>
public class MessageProcessorService : BackgroundService
{
    private readonly ChannelReader<EventMessage> _reader;
    private readonly ILogger<MessageProcessorService> _logger;
    private readonly PubSub.PubSubClient _grpcClient;
    private readonly IMemoryCache _memoryCache;

    public MessageProcessorService(ChannelReader<EventMessage> reader, ILogger<MessageProcessorService> logger, PubSub.PubSubClient grpcClient, IMemoryCache memoryCache)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _grpcClient = grpcClient ?? throw new ArgumentNullException(nameof(grpcClient));
        _memoryCache = memoryCache ?? throw new ArgumentNullException(nameof(memoryCache));
        
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogDebug("{Method} called on {Class}", nameof(ExecuteAsync), nameof(MessageProcessorService));
        
        await foreach (var message in _reader.ReadAllAsync(stoppingToken))
        {
            //the replay id is in big endian
            var replayId = BinaryPrimitives.ReadInt64BigEndian(message.ConsumerEvent.ReplayId.ToByteArray());
            var schemaId = message.ConsumerEvent.Event.SchemaId;

            //get the schema, this can load additional schemas if a schema is specified that wasn't cached by the topic subscription manager
            var cacheEntry = await _memoryCache.GetOrCreateAsync<SchemaInfo>(schemaId, async entry => await GetSchemaAsync(schemaId, stoppingToken));
            var des = AvroConvert.Avro2Json(message.ConsumerEvent.Event.Payload.ToByteArray(), cacheEntry.SchemaJson);
            
            //do something useful with the message
            _logger.LogDebug("Received: {ReplayId}/{Id}/{TopicName}", replayId, message.ConsumerEvent.Event.Id, message.TopicName);

        }

        _logger.LogDebug("{Method} exiting on {Class}", nameof(ExecuteAsync), nameof(MessageProcessorService));
    }
    
    /// <summary>
    /// gets a schema and caches in memcache
    /// </summary>
    /// <param name="schemaId"></param>
    /// <param name="stoppingToken"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    private async Task<SchemaInfo> GetSchemaAsync(string schemaId, CancellationToken stoppingToken = default)
    {
        if (string.IsNullOrEmpty(schemaId))
            throw new InvalidOperationException("Cannot retreive schema.  Topic info is not initialized");
        
        return await _grpcClient.GetSchemaAsync(new SchemaRequest() { SchemaId = schemaId }, cancellationToken: stoppingToken, deadline: DateTime.UtcNow.AddMinutes(1)).ConfigureAwait(false);
    }
}