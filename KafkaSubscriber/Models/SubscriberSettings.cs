using System.Threading;
using KafkaSubscriber.Interfaces;

namespace KafkaSubscriber.Models
{
    public class SubscriberSettings
    {
        public IPointService PointService { get; set; }
        public string KafkaTopic { get; set; }
        public string KafkaGroupId { get; set; }
        public AutoOffsetReset AutoOffsetReset { get; set; }
        public CancellationToken SubscriberCancellationToken { get; set; } = default(CancellationToken);
        public string KafkaServers { get; set; } = "sb-kaffka1.dc.wildberries.ru:9092, sb-kaffka2.dc.wildberries.ru: 9092, sb-kaffka3.dc.wildberries.ru:9092";
    }
}
