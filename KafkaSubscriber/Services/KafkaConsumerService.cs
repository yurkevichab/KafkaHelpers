using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using KafkaSubscriber.Models;

namespace KafkaSubscriber.Services
{
    public class KafkaConsumerService
    {
        private readonly SubscriberSettings _subscriberSettings;

        public KafkaConsumerService(SubscriberSettings subscriberSettings)
        {
            _subscriberSettings = subscriberSettings;
        }

        public async Task KafkaSubscribeAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                KafkaSubscribe(cancellationToken);
            });
        }

        public void KafkaSubscribe(CancellationToken cancellationToken = default(CancellationToken))
        {
            var config = new Dictionary<string, object>
            {
                { "group.id", _subscriberSettings.KafkaGroupId },
                { "enable.auto.commit", false },
                { "bootstrap.servers", _subscriberSettings.KafkaServers },
                { "default.topic.config", new Dictionary<string, object>
                    {
                        { "auto.offset.reset", _subscriberSettings.AutoOffsetReset.ToString().ToLower() }
                    }
                }
            };
            using (var consumer = new Consumer<string, string>(config, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                var bulk = new List<Message<string, string>>();
                consumer.OnMessage += (_, msg) =>
                {
                    var success = _subscriberSettings.PointService.SaveMessage(msg);

                    if (success == ProcessingStatus.Retry)
                    {
                        consumer.Unassign();
                        consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(_subscriberSettings.KafkaTopic, 0, msg.Offset) });
                    }
                    else if (success == ProcessingStatus.Commit)
                    {
                        var committedOffsets = consumer.CommitAsync(msg);
                    }
                    else if (success == ProcessingStatus.Collect)
                    {
                        bulk.Add(msg);
                    }
                };

                consumer.OnConsumeError += (_, msg)
                    => throw new Exception($"Error consuming from topic/partition/offset {msg.Topic}/{msg.Offset}: {msg.Error}");

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    consumer.Assign(partitions);
                };

                consumer.Subscribe(_subscriberSettings.KafkaTopic);


                while (!cancellationToken.IsCancellationRequested)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                    if (_subscriberSettings.PointService.SaveCollectedMessages(bulk))
                    {
                        var lastMsg = bulk.LastOrDefault();
                        if (lastMsg != null)
                        {
                            var committedOffsets = consumer.CommitAsync(lastMsg).GetAwaiter().GetResult();
                        }
                        bulk.Clear();
                    }
                }

            }
        }

    }
}
