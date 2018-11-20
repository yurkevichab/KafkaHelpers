using System.Threading;
using System.Threading.Tasks;

namespace KafkaSubscriber.Interfaces
{
    public interface ISynchronizationService
    {
        Task StartSynchronizationAsync(CancellationToken cancellationToken);
    }
}
