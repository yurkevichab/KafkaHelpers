using System.Collections.Generic;
using Confluent.Kafka;
using KafkaSubscriber.Models;

namespace KafkaSubscriber.Interfaces
{
    public interface IPointService
    {
        ProcessingStatus SaveMessage(Message<string, string> message);

        bool SaveCollectedMessages(List<Message<string, string>> messages);
    }
}
