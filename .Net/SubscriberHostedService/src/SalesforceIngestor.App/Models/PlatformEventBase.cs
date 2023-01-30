using System.Text.Json.Serialization;

namespace SalesforceIngestor.App.Models;

public class PlatformEventBase
{
    [JsonPropertyName("Source_Entity__c")]
    public string SourceEntity { get; set; }
    
    [JsonPropertyName("Operation_Id__c")]
    public Guid OperationId { get; set; }
    
    [JsonPropertyName("Correlation_Id__c")]
    public string CorrelationId { get; set; }
    
    [JsonPropertyName("Id__c")]
    public string Id { get; set; }
    
    // [JsonPropertyName("Last_Modified_On__c")]
    // public DateTimeOffset LastModifiedOn { get; set; }
    
    [JsonPropertyName("Last_Modified_By__c")]
    public string LastModifiedBy { get; set; }
    
    // [JsonPropertyName("CreatedDate")]
    // public DateTimeOffset CreatedDate { get; set; }
    
    [JsonPropertyName("CreatedById")]
    public string CreatedById { get; set; } 
}