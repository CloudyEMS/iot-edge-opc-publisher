using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Core;

namespace OpcPublisher
{
    public class PropertiesProcessor
    {
        private readonly Logger _logger;
        private readonly IHubClient _hubClient;
        private readonly bool _iotCentralMode;
        private readonly int _defaultSendIntervalSeconds;
        private readonly CancellationToken _shutdownToken;
        private static BlockingCollection<MessageData> _monitoredPropertiesDataQueue;
        
        private long _enqueueFailureCount;
        private long _enqueueCount;

        /// <summary>
        /// Specifies the queue capacity for monitored properties.
        /// </summary>
        public int MonitoredPropertiesQueueCapacity { get; set; } = 8192;

        /// <summary>
        /// Number of events in the monitored items queue.
        /// </summary>
        public long MonitoredPropertiesQueueCount => _monitoredPropertiesDataQueue?.Count ?? 0;

        /// <summary>
        /// Number of properties we sent to the cloud using deviceTwin
        /// </summary>
        public long SentProperties { get; set; }

        public PropertiesProcessor(Logger logger, IHubClient hubClient, bool iotCentralMode, int defaultSendIntervalSeconds, CancellationToken shutdownToken)
        {
            _logger = logger;
            _hubClient = hubClient;
            _iotCentralMode = iotCentralMode;
            _defaultSendIntervalSeconds = defaultSendIntervalSeconds;
            _shutdownToken = shutdownToken;
            
            _monitoredPropertiesDataQueue = new BlockingCollection<MessageData>(MonitoredPropertiesQueueCapacity);
        }
        

        public void EnqueueProperty(MessageData message)
        {
            Interlocked.Increment(ref _enqueueCount);
            if (_monitoredPropertiesDataQueue.TryAdd(message))
                return;
            Interlocked.Increment(ref _enqueueFailureCount);
            if (_enqueueFailureCount % 10000 == 0)
            {
                _logger.Information($"The internal monitored property message queue is above its capacity of {_monitoredPropertiesDataQueue.BoundedCapacity}. We have already lost {_enqueueFailureCount} monitored item notifications:(");
            }
        }

        public Task MonitoredPropertiesProcessorAsync()
        {
            if (!_iotCentralMode) return Task.CompletedTask;
            
            DateTime nextSendTime = DateTime.UtcNow + TimeSpan.FromSeconds(_defaultSendIntervalSeconds);
            double millisecondsTillNextSend = nextSendTime.Subtract(DateTime.UtcNow).TotalMilliseconds;

            while (!_shutdownToken.IsCancellationRequested)
            {
                try
                {        
                    // sanity check the send interval, compute the timeout and get the next monitored item message
                    if (_defaultSendIntervalSeconds > 0)
                    {
                        millisecondsTillNextSend = nextSendTime.Subtract(DateTime.UtcNow).TotalMilliseconds;
                        if (millisecondsTillNextSend < 0)
                        {
                            // do not wait if we missed the send interval
                            millisecondsTillNextSend = 0;
                        }
                    }
                    else
                    {
                        // if we are in shutdown do not wait, else wait infinite if send interval is not set
                        millisecondsTillNextSend = _shutdownToken.IsCancellationRequested ? 0 : Timeout.Infinite;
                    }

                    var gotItem = _monitoredPropertiesDataQueue.TryTake(out var messageData, (int)millisecondsTillNextSend, _shutdownToken);

                    if (!gotItem || messageData == null) continue;
                    _hubClient.SendPropertyAsync(messageData, _shutdownToken).GetAwaiter().GetResult();

                    SentProperties++;
                }
                catch (Exception e)
                {
                    _logger.Error(e, "Error while processing monitored properties.");
                }
            }

            return Task.CompletedTask;
        }
    }
}
