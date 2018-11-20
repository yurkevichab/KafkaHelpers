using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaSubscriber.Interfaces;
using KafkaSubscriber.Models;
using Microsoft.Extensions.Logging;

namespace KafkaSubscriber.Services
{
    public class SynchronizationService : ISynchronizationService
    {
        private readonly IEnumerable<SubscriberSettings> _subscriberSettingsList;
        private readonly ILogger<SynchronizationService> _logger;
        private readonly TimeSpan _restartDelay = TimeSpan.FromMinutes(1);

        public SynchronizationService(IEnumerable<SubscriberSettings> subscriberSettingsList, ILoggerFactory loggerFactory)
        {
            _subscriberSettingsList = subscriberSettingsList;
            _logger = loggerFactory.CreateLogger<SynchronizationService>();
        }

        public Task StartSynchronizationAsync(CancellationToken cancellationToken)
        {
            try
            {
                if (_subscriberSettingsList.Any())
                {
                    var task = Task.Run(() =>
                    {
                        
                        var delegateList = _subscriberSettingsList.Select(CreateKafkaSendDataAction).ToArray();
                        var tasks = delegateList.Select(action => action()).ToList();
                        while (true)
                        {
                            var index = Task.WaitAny(tasks.ToArray());
                            var failTask = tasks[index];
                            _logger.LogError(failTask.Exception.Message);
                            Task.Delay(_restartDelay).GetAwaiter().GetResult();
                            tasks[index] = delegateList[index]();
                        }
                    },
                        cancellationToken);
                    return task;
                }
                return Task.CompletedTask;

            }
            catch (Exception e)
            {
                _logger.LogError(e.ToString());
            }
            return Task.CompletedTask;
        }

        //private void Run()
        //{
        //    var delegateList = _subscriberSettingsList.Select(CreateKafkaSendDataAction).ToArray();
        //    var tasks = delegateList.Select(action => action()).ToList();
        //    while (true)
        //    {
        //        var index = Task.WaitAny(tasks.ToArray());
        //        var failTask = tasks[index];
        //        _logger.LogError(failTask.Exception.Message);
        //        Task.Delay(_restartDelay).GetAwaiter().GetResult();
        //        tasks[index] = delegateList[index]();
        //    }
        //}

        private Func<Task> CreateKafkaSendDataAction(SubscriberSettings subscriberSettings)
        {
            var kafkaConsumerService = new KafkaConsumerService(subscriberSettings);
            return () => kafkaConsumerService.KafkaSubscribeAsync(subscriberSettings.SubscriberCancellationToken);
        }
    }
}
