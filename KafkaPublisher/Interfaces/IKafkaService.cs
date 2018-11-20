using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaPublisher.Interfaces
{
    public interface IKafkaService
    {
        Task SendDataInKafkaAsync(List<Tuple<string, string>> data, string kafkaTopic, CancellationToken cancellationToken = default(CancellationToken));
    }
}