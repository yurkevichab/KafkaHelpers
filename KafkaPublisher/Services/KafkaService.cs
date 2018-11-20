using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using KafkaPublisher.Interfaces;
using System.Threading;

namespace KafkaPublisher.Services
{
    public class KafkaService : IKafkaService
    {
        private const int MaxTasksLimit = 10000;
        private static Producer<string, string> _producer;

        public KafkaService(string kafkaServers)
        {
            _producer = InitializeProducer(kafkaServers);
        }

        /// <summary>
        ///    Send messages list in kafka topic
        /// </summary>
        /// <param name="datas">Tuple is object what send in kafka, where
        /// item1 is key for message, item2 is json message</param>
        /// <param name="kafkaTopic">Name of kafkas topic</param>
        /// <returns></returns>
        public async Task SendDataInKafkaAsync(List<Tuple<string, string>> datas, string kafkaTopic, CancellationToken cancellationToken = default(CancellationToken))
        {
            var taskList = new List<Task>();
            var lastIndex = datas.Count - 1;
            for (int i = 0; i <= lastIndex && !cancellationToken.IsCancellationRequested; i++)
            {
                taskList.Add(_producer.ProduceAsync(kafkaTopic, datas[i].Item1,
                        datas[i].Item2));
                if (i == lastIndex || taskList.Count == MaxTasksLimit)
                {
                    await Task.WhenAll(taskList.ToArray());
                    taskList.Clear();
                }
            }
            _producer.Flush(1000);
        }

        private static Producer<string, string> InitializeProducer(string kafkaServers)
        {
            var config = new Dictionary<string, object>
            {
                {
                    "bootstrap.servers", kafkaServers
                }
            };
            return _producer ?? new Producer<string, string>(config, new StringSerializer(Encoding.UTF8),
                       new StringSerializer(Encoding.UTF8));
        }
    }
}