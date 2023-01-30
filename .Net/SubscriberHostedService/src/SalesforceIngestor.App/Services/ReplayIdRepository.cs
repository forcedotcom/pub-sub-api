namespace SalesforceIngestor.App.Services;

public interface IReplayIdRepository
{
    Task<long> GetReplayIdAsync(string topicName);
    Task UpdateReplayIdAsync(string topicName, long replayId);
}