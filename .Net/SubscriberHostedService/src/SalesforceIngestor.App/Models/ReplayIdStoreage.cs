using System.Text.Json.Serialization;

namespace SalesforceIngestor.App.Models;

public class ReplayIdData {
    public long ReplayId { get; set; }
    public string? TopicName { get; set; }
    
    [JsonIgnore]
    public bool IsUpdated { get; set; }
}

internal class  ReplayIdStoreage
{
    public List<ReplayIdData>? ReplayIdData { get; set; }
    public DateTimeOffset? StoredOn { get; set; }
}