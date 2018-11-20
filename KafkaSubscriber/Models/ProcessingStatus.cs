namespace KafkaSubscriber.Models
{
    public enum ProcessingStatus
    {
        Retry,
        Commit,
        Collect
    }
}
