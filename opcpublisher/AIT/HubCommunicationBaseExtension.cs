using Serilog.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.IIoT.Modules.OpcUa.Publisher.AIT;
using Newtonsoft.Json;
using Opc.Ua;

namespace OpcPublisher
{
    using static OpcApplicationConfiguration;
    using static Program;

    public partial class HubCommunicationBase
    {
        public Task<bool> InitExtendedProcessingAsync(Logger logger)
        {
            try
            {
                _logger = logger;
                
                _monitoredPropertiesDataQueue = new BlockingCollection<MessageData>(MonitoredPropertiesQueueCapacity);
                _monitoredSettingsDataQueue = new BlockingCollection<MessageData>(MonitoredSettingsQueueCapacity);

                return Task.FromResult(true);
            }
            catch (Exception e)
            {
                _logger.Error(e, "Failure initializing property processing.");
                return Task.FromResult(false);
            }
        }

        public async Task MonitoredPropertiesProcessorAsync()
        {
            try
            {
                if (!IotCentralMode) return;

                var gotItem = _monitoredPropertiesDataQueue.TryTake(out MessageData messageData, DefaultSendIntervalSeconds, _shutdownToken);

                if (!gotItem || messageData == null) return;
                await _hubClient.SendPropertyAsync(messageData, _shutdownToken);
                SentProperties++;
            }
            catch (Exception e)
            {
                _logger.Error(e, "Error while processing monitored properties.");
                throw;
            }
        }

        public Task MonitoredSettingsProcessorAsync()
        {
            try
            {
                if (!IotCentralMode) return Task.CompletedTask;

                var gotItem = _monitoredSettingsDataQueue.TryTake(out var messageData);

                if (!gotItem || messageData == null) return Task.CompletedTask;
                if (!HubClient.MonitoredSettingsCollection.TryAdd(
                    messageData.DataChangeMessageData.DisplayName, messageData.DataChangeMessageData.Value))
                {
                    _logger.Error("Error while storing a new monitored setting.");
                }

                SentSettings++;
                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                _logger.Error(e, "Error while processing monitored settings.");
                throw;
            }
        }

        public void EnqueueProperty(MessageData message)
        {
            Interlocked.Increment(ref _enqueueCount);
            if (_monitoredPropertiesDataQueue.TryAdd(message) == false)
            {
                Interlocked.Increment(ref _enqueueFailureCount);
                if (_enqueueFailureCount % 10000 == 0)
                {
                    _logger.Information($"The internal monitored property message queue is above its capacity of {_monitoredPropertiesDataQueue.BoundedCapacity}. We have already lost {_enqueueFailureCount} monitored item notifications:(");
                }
            }
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

        public void EnqueueEvent(MessageData message)
        {
            _iotcEventsProcessor.EnqueueEvent(message);
        }

        /// <summary>
        /// Handle publish event node method call.
        /// </summary>
        public virtual async Task<MethodResponse> HandlePublishEventsMethodAsync(MethodRequest methodRequest,
            object userContext)
        {
            var logPrefix = "HandlePublishEventsMethodAsync:";
            var useSecurity = true;
            Uri endpointUri = null;
            PublishNodesMethodRequestModel publishEventsMethodData = null;
            var statusCode = HttpStatusCode.OK;
            var statusResponse = new List<string>();
            string statusMessage;
            try
            {
                _logger.Debug($"{logPrefix} called");
                publishEventsMethodData = JsonConvert.DeserializeObject<PublishNodesMethodRequestModel>(methodRequest.DataAsJson);
                endpointUri = new Uri(publishEventsMethodData.EndpointUrl);
                useSecurity = publishEventsMethodData.UseSecurity;

                if (publishEventsMethodData.OpcEvents.Count != 1)
                {
                    statusMessage =
                        $"You can only configure one Event simultaneously, but you trying to configure {publishEventsMethodData.OpcEvents.Count + 1} events";
                    _logger.Error($"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.NotAcceptable;
                }
            }
            catch (UriFormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointUrl '{publishEventsMethodData?.EndpointUrl}'";
                _logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.NotAcceptable;
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while deserializing message payload";
                _logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            if (statusCode == HttpStatusCode.OK)
            {
                // find/create a session to the endpoint URL and start monitoring the node.
                try
                {
                    // lock the publishing configuration till we are done
                    await NodeConfiguration.OpcSessionsListSemaphore.WaitAsync(_shutdownToken).ConfigureAwait(false);

                    if (ShutdownTokenSource.IsCancellationRequested)
                    {
                        statusMessage = $"Publisher is in shutdown";
                        _logger.Warning($"{logPrefix} {statusMessage}");
                        statusResponse.Add(statusMessage);
                        statusCode = HttpStatusCode.Gone;
                    }
                    else
                    {
                        // find the session we need to monitor the node
                        IOpcSession opcSession = null;
                        opcSession = NodeConfiguration.OpcSessions.FirstOrDefault(s => s.EndpointUrl.Equals(endpointUri?.OriginalString, StringComparison.OrdinalIgnoreCase));

                        // add a new session.
                        if (opcSession == null)
                        {
                            if (publishEventsMethodData?.OpcAuthenticationMode != null)
                            {
                                var authenticationMode = publishEventsMethodData.OpcAuthenticationMode == null
                                    ? publishEventsMethodData.OpcAuthenticationMode.Value
                                    : OpcAuthenticationMode.Anonymous;

                                var encryptedCredentials = await Crypto.EncryptedNetworkCredential.FromPlainCredential(publishEventsMethodData.UserName, publishEventsMethodData.Password);

                                // create new session info.
                                opcSession = new OpcSession(endpointUri?.OriginalString, useSecurity, OpcSessionCreationTimeout,
                                    authenticationMode, encryptedCredentials);
                            }

                            NodeConfiguration.OpcSessions.Add(opcSession);
                            Logger.Information($"{logPrefix} No matching session found for endpoint '{endpointUri?.OriginalString}'. Requested to create a new one.");
                        }

                        // process all nodes
                        if (publishEventsMethodData?.OpcEvents != null)
                        {
                            foreach (var eventNode in publishEventsMethodData?.OpcEvents)
                            {
                                NodeId nodeId = null;
                                ExpandedNodeId expandedNodeId = null;
                                bool isNodeIdFormat;
                                try
                                {
                                    if (eventNode.Id.Contains("nsu=", StringComparison.InvariantCulture))
                                    {
                                        expandedNodeId = ExpandedNodeId.Parse(eventNode.Id);
                                        isNodeIdFormat = false;
                                    }
                                    else
                                    {
                                        nodeId = NodeId.Parse(eventNode.Id);
                                        isNodeIdFormat = true;
                                    }
                                }
                                catch (Exception e)
                                {
                                    statusMessage = $"Exception in ({e.Message}) while formatting node '{eventNode.Id}'!";
                                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                                    statusResponse.Add(statusMessage);
                                    statusCode = HttpStatusCode.NotAcceptable;
                                    continue;
                                }

                                try
                                {
                                    HttpStatusCode nodeStatusCode;
                                    if (isNodeIdFormat)
                                    {
                                        // add the event node info to the subscription with the default publishing interval, execute synchronously
                                        Logger.Debug(
                                            $"{logPrefix} Request to monitor eventNode with NodeId '{eventNode.Id}'");
                                        nodeStatusCode = await opcSession.AddEventNodeForMonitoringAsync(nodeId, null, 5000,
                                                2000, eventNode.DisplayName, null, null, ShutdownTokenSource.Token,
                                                null, publishEventsMethodData)
                                            .ConfigureAwait(false);
                                    }
                                    else
                                    {
                                        // add the event node info to the subscription with the default publishing interval, execute synchronously
                                        Logger.Debug(
                                            $"{logPrefix} Request to monitor eventNode with ExpandedNodeId '{eventNode.Id}'");
                                        nodeStatusCode = await opcSession.AddEventNodeForMonitoringAsync(null,
                                                expandedNodeId, 5000, 2000, eventNode.DisplayName, 
                                                null, null, ShutdownTokenSource.Token, null, 
                                                publishEventsMethodData)
                                            .ConfigureAwait(false);
                                    }

                                    // check and store a result message in case of an error
                                    switch (nodeStatusCode)
                                    {
                                        case HttpStatusCode.OK:
                                            statusMessage = $"'{eventNode.Id}': already monitored";
                                            Logger.Debug($"{logPrefix} {statusMessage}");
                                            statusResponse.Add(statusMessage);
                                            break;

                                        case HttpStatusCode.Accepted:
                                            statusMessage = $"'{eventNode.Id}': added";
                                            Logger.Debug($"{logPrefix} {statusMessage}");
                                            statusResponse.Add(statusMessage);
                                            break;

                                        case HttpStatusCode.Gone:
                                            statusMessage =
                                                $"'{eventNode.Id}': session to endpoint does not exist anymore";
                                            Logger.Debug($"{logPrefix} {statusMessage}");
                                            statusResponse.Add(statusMessage);
                                            statusCode = HttpStatusCode.Gone;
                                            break;

                                        case HttpStatusCode.InternalServerError:
                                            statusMessage = $"'{eventNode.Id}': error while trying to configure";
                                            Logger.Debug($"{logPrefix} {statusMessage}");
                                            statusResponse.Add(statusMessage);
                                            statusCode = HttpStatusCode.InternalServerError;
                                            break;
                                    }
                                }
                                catch (Exception e)
                                {
                                    statusMessage =
                                        $"Exception ({e.Message}) while trying to configure publishing node '{eventNode.Id}'";
                                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                                    statusResponse.Add(statusMessage);
                                    statusCode = HttpStatusCode.InternalServerError;
                                }
                            }
                        }
                        else
                        {
                            statusMessage =
                                $"There are no EventConfigurations provided with the current call, provided JSON Data was: {methodRequest.DataAsJson}";
                            Logger.Error($"{logPrefix} {statusMessage}");
                            statusResponse.Add(statusMessage);
                            statusCode = HttpStatusCode.BadRequest;
                        }
                    }
                }
                catch (AggregateException e)
                {
                    foreach (var ex in e.InnerExceptions)
                    {
                        Logger.Error(ex, $"{logPrefix} Exception");
                    }
                    statusMessage = $"EndpointUrl: '{publishEventsMethodData.EndpointUrl}': exception ({e.Message}) while trying to publish";
                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
                catch (Exception e)
                {
                    statusMessage = $"EndpointUrl: '{publishEventsMethodData.EndpointUrl}': exception ({e.Message}) while trying to publish";
                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
                finally
                {
                    NodeConfiguration.OpcSessionsListSemaphore.Release();
                }
            }

            // adjust response size
            AdjustResponse(ref statusResponse);

            // build response
            var resultString = JsonConvert.SerializeObject(statusResponse);
            var result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return methodResponse;
        }

        /// <summary>
        /// Handle method call to get list of configured nodes on a specific endpoint.
        /// </summary>
        public virtual Task<MethodResponse> HandleGetConfiguredEventsOnEndpointMethodAsync(MethodRequest methodRequest, object userContext)
        {
            const string logPrefix = "HandleGetConfiguredNodesOnEndpointMethodAsync:";
            Uri endpointUri = null;
            GetConfiguredNodesOnEndpointMethodRequestModel getConfiguredEventNodesOnEndpointMethodRequest = null;
            uint nodeConfigVersion = 0;
            GetConfiguredEventNodesOnEndpointMethodResponseModel getConfiguredEventNodesOnEndpointMethodResponse = new GetConfiguredEventNodesOnEndpointMethodResponseModel();
            uint actualNodeCount = 0;
            uint availableEventNodeCount = 0;
            var opcEvents = new List<OpcEventOnEndpointModel>();
            uint startIndex = 0;
            var statusCode = HttpStatusCode.OK;
            var statusResponse = new List<string>();
            string statusMessage;

            try
            {
                Logger.Debug($"{logPrefix} called");
                getConfiguredEventNodesOnEndpointMethodRequest = JsonConvert.DeserializeObject<GetConfiguredNodesOnEndpointMethodRequestModel>(methodRequest.DataAsJson);
                endpointUri = new Uri(getConfiguredEventNodesOnEndpointMethodRequest.EndpointUrl);
            }
            catch (UriFormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointUrl '{getConfiguredEventNodesOnEndpointMethodRequest?.EndpointUrl}'";
                Logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while deserializing message payload";
                Logger.Error(e, $"{logPrefix} Exception");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            if (statusCode == HttpStatusCode.OK)
            {
                // get the list of published nodes for the endpoint
                List<PublisherConfigurationFileEntryModel> configFileEntries = NodeConfiguration.GetPublisherConfigurationFileEntries(endpointUri?.OriginalString, false, out nodeConfigVersion);

                // return if there are no nodes configured for this endpoint
                if (configFileEntries.Count == 0)
                {
                    statusMessage = $"There are no event nodes configured for endpoint '{endpointUri?.OriginalString}'";
                    Logger.Information($"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.OK;
                }
                else
                {
                    foreach (var configFileEntry in configFileEntries)
                    {
                        opcEvents.AddRange(configFileEntry.OpcEvents);
                    }
                    uint configuredEventNodesOnEndpointCount = (uint)opcEvents.Count();

                    // validate version
                    startIndex = 0;
                    if (getConfiguredEventNodesOnEndpointMethodRequest?.ContinuationToken != null)
                    {
                        uint requestedNodeConfigVersion = (uint)(getConfiguredEventNodesOnEndpointMethodRequest.ContinuationToken >> 32);
                        if (nodeConfigVersion != requestedNodeConfigVersion)
                        {
                            statusMessage = $"The event node configuration has changed between calls. Requested version: {requestedNodeConfigVersion:X8}, Current version '{nodeConfigVersion:X8}'!";
                            Logger.Information($"{logPrefix} {statusMessage}");
                            statusResponse.Add(statusMessage);
                            statusCode = HttpStatusCode.Gone;
                        }
                        startIndex = (uint)(getConfiguredEventNodesOnEndpointMethodRequest.ContinuationToken & 0x0FFFFFFFFL);
                    }

                    if (statusCode == HttpStatusCode.OK)
                    {
                        // set count
                        var requestedEventNodeCount = configuredEventNodesOnEndpointCount - startIndex;
                        availableEventNodeCount = configuredEventNodesOnEndpointCount - startIndex;
                        actualNodeCount = Math.Min(requestedEventNodeCount, availableEventNodeCount);

                        // generate response
                        while (true)
                        {
                            string publishedNodesString = JsonConvert.SerializeObject(opcEvents.GetRange((int)startIndex, (int)actualNodeCount));
                            var publishedNodesByteArray = Encoding.UTF8.GetBytes(publishedNodesString);
                            if (publishedNodesByteArray.Length > MaxResponsePayloadLength)
                            {
                                actualNodeCount /= 2;
                                continue;
                            }

                            break;
                        }
                    }
                }
            }

            // build response
            string resultString;
            if (statusCode == HttpStatusCode.OK)
            {
                getConfiguredEventNodesOnEndpointMethodResponse.ContinuationToken = null;
                if (actualNodeCount < availableEventNodeCount)
                {
                    getConfiguredEventNodesOnEndpointMethodResponse.ContinuationToken = (ulong)nodeConfigVersion << 32 | actualNodeCount + startIndex;
                }

                getConfiguredEventNodesOnEndpointMethodResponse.EventNodes.AddRange(opcEvents
                    .GetRange((int)startIndex, (int)actualNodeCount).Select(n =>
                        new OpcEventOnEndpointModel(new EventConfigurationModel(endpointUri?.OriginalString,
                            null, n.Id, n.DisplayName, n.SelectClauses, n.WhereClause, n.IotCentralEventPublishMode))));
                getConfiguredEventNodesOnEndpointMethodResponse.EndpointUrl = endpointUri?.OriginalString;
                resultString = JsonConvert.SerializeObject(getConfiguredEventNodesOnEndpointMethodResponse);
                Logger.Information($"{logPrefix} Success returning {actualNodeCount} event node(s) of {availableEventNodeCount} (start: {startIndex}) (node config version: {nodeConfigVersion:X8})!");
            }
            else
            {
                resultString = JsonConvert.SerializeObject(statusResponse);
            }
            byte[] result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return Task.FromResult(methodResponse);
        }
        
        /// <summary>
        /// Number of times we were not able to make the settings send interval, because too high load.
        /// </summary>
        public static long MissedSettingSendIntervalCount { get; set; }

        /// <summary>
        /// Number of times we were not able to make the property send interval, because too high load.
        /// </summary>
        public static long MissedPropertySendIntervalCount { get; set; }

        /// <summary>
        /// Number of properties we sent to the cloud using deviceTwin
        /// </summary>
        public static long SentProperties { get; set; }

        /// <summary>
        /// Number of settings we sent to the cloud using deviceTwin
        /// </summary>
        public static long SentSettings { get; set; }

        public static long SentIoTcEvents => IoTCEventsProcessor.SentIoTcEvents;

        /// <summary>
        /// Specifies the queue capacity for monitored properties.
        /// </summary>
        public static int MonitoredPropertiesQueueCapacity { get; set; } = 8192;

        /// <summary>
        /// Specifies the queue capacity for monitored settings.
        /// </summary>
        public static int MonitoredSettingsQueueCapacity { get; set; } = 8192;

        /// <summary>
        /// Number of events in the monitored items queue.
        /// </summary>
        public static long MonitoredPropertiesQueueCount => _monitoredPropertiesDataQueue?.Count ?? 0;

        /// <summary>
        /// Number of events in the monitored items queue.
        /// </summary>
        public static long MonitoredSettingsQueueCount => _monitoredSettingsDataQueue?.Count ?? 0;

        public static TransportType SendHubProtocol { get; set; } = IotHubProtocolDefault;
        private Thread _monitoredPropertiesProcessorThread { get; set; }
        private Thread _monitoredSettingsProcessorThread { get; set; }
        private Thread _monitoredEventsProcessorThread { get; set; }

        private static BlockingCollection<MessageData> _monitoredPropertiesDataQueue;
        private static BlockingCollection<MessageData> _monitoredSettingsDataQueue;
        private static Logger _logger;
    }
}
