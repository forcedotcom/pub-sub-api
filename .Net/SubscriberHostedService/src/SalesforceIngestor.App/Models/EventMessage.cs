using PubSubApi;

namespace SalesforceIngestor.App.Models;

public class EventMessage
{
    public EventMessage(string topicName, ConsumerEvent consumerEvent)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentException("cannot be null or whitespace", nameof(topicName));
        
        TopicName = topicName;
        ConsumerEvent = consumerEvent ?? throw new ArgumentNullException(nameof(consumerEvent));
    }
    
    public string TopicName { get; }
    public string TopicFriendlyName { get; }
    public ConsumerEvent ConsumerEvent { get; }
}