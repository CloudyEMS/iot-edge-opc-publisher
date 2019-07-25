using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Core;

namespace OpcPublisher
{
    public class SettingsProcessor
    {
        private readonly Logger _logger;
        private readonly IHubClient _hubClient;
        private readonly bool _iotCentralMode;
        private readonly int _defaultSendIntervalSeconds;
        private readonly CancellationToken _shutdownToken;
        private static BlockingCollection<MessageData> _monitoredSettingsDataQueue;
        
        private long _enqueueFailureCount;
        private long _enqueueCount;

        /// <summary>
        /// Specifies the queue capacity for monitored settings.
        /// </summary>
        public int MonitoredSettingsQueueCapacity { get; set; } = 8192;

        /// <summary>
        /// Number of events in the monitored items queue.
        /// </summary>
        public long MonitoredSettingsQueueCount => _monitoredSettingsDataQueue?.Count ?? 0;

        /// <summary>
        /// Number of settings we sent to the cloud using deviceTwin
        /// </summary>
        public long SentSettings { get; set; }

        public SettingsProcessor(Logger logger, IHubClient hubClient, bool iotCentralMode, int defaultSendIntervalSeconds, CancellationToken shutdownToken)
        {
            _logger = logger;
            _hubClient = hubClient;
            _iotCentralMode = iotCentralMode;
            _defaultSendIntervalSeconds = defaultSendIntervalSeconds;
            _shutdownToken = shutdownToken;
            
            _monitoredSettingsDataQueue = new BlockingCollection<MessageData>(MonitoredSettingsQueueCapacity);
        }
        

        public void EnqueueSetting(MessageData message)
        {
            Interlocked.Increment(ref _enqueueCount);
            if (_monitoredSettingsDataQueue.TryAdd(message))
                return;
            Interlocked.Increment(ref _enqueueFailureCount);
            if (_enqueueFailureCount % 10000 == 0)
            {
                _logger.Information($"The internal monitored setting message queue is above its capacity of {_monitoredSettingsDataQueue.BoundedCapacity}. We have already lost {_enqueueFailureCount} monitored item notifications:(");
            }
        }

        public Task MonitoredSettingsProcessorAsync()
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

                    var gotItem = _monitoredSettingsDataQueue.TryTake(out var messageData, (int)millisecondsTillNextSend, _shutdownToken);

                    if (!gotItem || messageData == null) continue;
                    _hubClient.SendSettingAsync(messageData, _shutdownToken).GetAwaiter().GetResult();

                    SentSettings++;
                }
                catch (Exception e)
                {
                    _logger.Error(e, "Error while processing monitored settings.");
                }
            }

            return Task.CompletedTask;
        }
    }
}
