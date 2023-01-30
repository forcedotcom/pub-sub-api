using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SalesforceIngestor.App.Services;
using SalesforceIngestor.App.Models;

namespace SalesforceIngestor.App.BackgroundServices;
/// <summary>
/// Simple service to store replay ids to a local file.  Keeps data in memory and periodically writes updates to the file
/// </summary>
public class ReplayIdService : BackgroundService, IReplayIdRepository
{
    private readonly ILogger<ReplayIdService> _logger;

    private const string FileName = "replayIdStorage.txt"; //TODO: this should be made configurable
    private TimeSpan _waitInterval = TimeSpan.FromSeconds(60); 
    
    private static readonly SemaphoreSlim Lock = new SemaphoreSlim(1);
    private ReplayIdStoreage? _current;


    public ReplayIdService(ILogger<ReplayIdService> logger)
    {
        _logger = logger;
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await InitializeAsync().ConfigureAwait(false);
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {


        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(_waitInterval, stoppingToken).ConfigureAwait(false);
                await StoreReplayIdsAsync().ConfigureAwait(false);
            }
        }
        catch (TaskCanceledException e)
        {
            //swallow this, it is most likely from the stopping token in the Task.Delay await above
        }
        
        //call one last time to get any updates stored before shutdown
        await StoreReplayIdsAsync().ConfigureAwait(false);
        _logger.LogDebug("{Method} exiting on {Class}", nameof(ExecuteAsync), nameof(ReplayIdService));
    }

    private async Task InitializeAsync()
    {
        try
        {
            //create the file if it doesn't exist
            if (!File.Exists(FileName))
            {
                _logger.LogDebug("Replay Id does not exist.  Creating the file {FileName}", FileName);
                using (File.Create(FileName)) {}
            }
            
            
            _logger.LogDebug("Initializing Replay Id storage");
            await Lock.WaitAsync().ConfigureAwait(false);

            if (_current != null)
                return;

            //get the last line of the file(last record written).  This will be held in memory and periodically written
            //back to the file
            var lines = File.ReadLines(FileName).ToList();

            if (lines.Any())
            {
                var last = lines.Last();
                _current = JsonSerializer.Deserialize<ReplayIdStoreage>(last);
            }
            else
            {
                //if the file is empty/new just instatiate the storage as empty
                _current = new ReplayIdStoreage() { ReplayIdData = new List<ReplayIdData>() };
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "An error ocurred while initializing replay id repository");
            throw;
        }
        finally
        {
            Lock.Release();
        }

        _logger.LogDebug("Initializing Replay Id storage complete!");
    }

    /// <summary>
    /// Gets the stored replay id for a topic.  If nothing is stored for a topic an new entry will be added and returned
    /// with a replay id of -1.  For this application an -1 replay id indicates to request new events only
    /// </summary>
    /// <param name="topicName"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException"></exception>
    public async Task<long> GetReplayIdAsync(string topicName)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentNullException(nameof(topicName));

        _logger.LogDebug("Getting replay id for {Topic}", topicName);

        if (_current == null)
            await InitializeAsync().ConfigureAwait(false);

        var item = _current!.ReplayIdData!.FirstOrDefault(x => x.TopicName == topicName);

        if (item == null)
        {
            try
            {
                _logger.LogDebug(
                    "No replay id storage exists for Topic: {Topic}, creating new with default: {DefaultReplayId}",
                    topicName, Resources.DefaultReplayId);
                await Lock.WaitAsync().ConfigureAwait(false);
                item = new ReplayIdData()
                    { TopicName = topicName, ReplayId = Resources.DefaultReplayId, IsUpdated = true };
                _current!.ReplayIdData!.Add(item);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while getting replay id for topic with name: {Name}",
                    topicName);
                throw;
            }

            finally
            {
                Lock.Release();
            }
        }

        _logger.LogDebug("Replay Id: {ReplayId} returned for Topic: {Topic}", item.ReplayId, topicName);
        return item.ReplayId;
    }

    /// <summary>
    /// Updates the replay id for a given topic.  The the salesforce pub/sub api, the fetch response returns updated
    /// replay ids for topics even when events have not occurred.  This probably is designed to move subscribers further
    /// ahead on replay id storage.  Replay ids don't appear to be contiguous per topic, so it is likely sending the last
    /// known used replay id from a pool that is used for all events
    /// </summary>
    /// <param name="topicName"></param>
    /// <param name="replayId"></param>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public async Task UpdateReplayIdAsync(string topicName, long replayId)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentNullException(nameof(topicName));

        if (replayId < Resources.DefaultReplayId)
            throw new ArgumentOutOfRangeException(nameof(replayId), "Replay Id cannot be less than -1");

        try
        {
            await Lock.WaitAsync().ConfigureAwait(false);
            var item = _current!.ReplayIdData!.First(x => x.TopicName == topicName);
            if (item.ReplayId < replayId)
            {
                _logger.LogDebug("Updated replay id for topic: {Topic}", topicName);
                item.ReplayId = replayId;
                item.IsUpdated = true;  // mark as updated.  It will be stored back to file system at a later point in time
            }
            else
            {
                _logger.LogDebug("No replay id updates");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occured update replay id for topic: {Name}", topicName);
        }
        finally
        {
            Lock.Release();
        }
    }

    /// <summary>
    /// Append the replay id data back to the 
    /// </summary>
    private async Task StoreReplayIdsAsync()
    {
        try
        {
            await Lock.WaitAsync().ConfigureAwait(false);

            if (_current!.ReplayIdData!.Any(x => x.IsUpdated))
            {
                _logger.LogDebug("Storing updated replay ids");

                _current.StoredOn = DateTimeOffset.UtcNow;
                var toAppend =
                    JsonSerializer.Serialize(_current, new JsonSerializerOptions() { WriteIndented = false });
                await File.AppendAllLinesAsync(FileName, new[] { toAppend }).ConfigureAwait(false);

                foreach (var item in _current!.ReplayIdData!)
                {
                    item.IsUpdated = false;
                }
            }
            else
            {
                _logger.LogDebug("No updated replay id entries found. Nothing to store");
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error committing replay ids to file");
            throw;
        }
        finally
        {
            Lock.Release();
        }
    }
}