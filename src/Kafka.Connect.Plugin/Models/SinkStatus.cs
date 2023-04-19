namespace Kafka.Connect.Plugin.Models
{
    public enum SinkStatus
    {
        Empty,
        
        Polling,
        Consumed,
        
        Processing,
        Processed,
        
        Updating,
        Updated,
        
        Inserting,
        Inserted,
        
        Deleting,
        Deleted,
        
        Skipping,
        Skipped,
        
        Failed,
        
        Document,
        
        Enriching,
        Enriched,
        
        Excluding,
        Excluded,
        
        Publishing,
        Published
    }
}