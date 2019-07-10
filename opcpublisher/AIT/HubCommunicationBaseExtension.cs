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
        public Task<bool> InitExtendedProcessingAsync(Logger logger, IHubClient hubClient)
        {
            try
            {
                _logger = logger;
                _logger.Information($"Property processing configured with a send interval of {DefaultSendIntervalSeconds} sec");
                _monitoredPropertiesDataQueue = new BlockingCollection<MessageData>(MonitoredPropertiesQueueCapacity);
                _monitoredSettingsDataQueue = new BlockingCollection<MessageData>(MonitoredSettingsQueueCapacity);
                _monitoredIoTcEventDataQueue = new BlockingCollection<MessageData>(MonitoredSettingsIoTcEventCapacity);

                _logger.Information("Creating task process for monitored property data updates...");
                _monitoredPropertiesProcessorThread = new Thread(async () => await MonitoredPropertiesProcessorAsync(hubClient));
                _monitoredPropertiesProcessorThread.Start();
                
                _logger.Information("Creating task process for monitored setting data updates...");
                _monitoredSettingsProcessorThread = new Thread(async () => await MonitoredSettingsProcessor(hubClient));
                _monitoredSettingsProcessorThread.Start();              
                
                _logger.Information("Creating task process for monitored event data updates...");
                _monitoredEventsProcessorThread = new Thread(async () => await MonitoredIoTCEventsProcessorAsync(hubClient, _shutdownToken));
                _monitoredEventsProcessorThread.Start();

                return Task.FromResult(true);
            }
            catch (Exception e)
            {
                _logger.Error(e, "Failure initializing property processing.");
                return Task.FromResult(false);
            }
        }

        public async Task MonitoredPropertiesProcessorAsync(IHubClient hubClient)
        {
            var nextSendTime = DateTime.UtcNow + TimeSpan.FromSeconds(DefaultSendIntervalSeconds);

            try
            {
                while (true)
                {
                    // sanity check the send interval, compute the timeout and get the next monitored item message
                    double millisecondsTillNextSend;
                    if (DefaultSendIntervalSeconds > 0)
                    {
                        millisecondsTillNextSend = nextSendTime.Subtract(DateTime.UtcNow).TotalMilliseconds;
                        if (millisecondsTillNextSend < 0)
                        {
                            MissedPropertySendIntervalCount++;
                            // do not wait if we missed the send interval
                            millisecondsTillNextSend = 0;
                        }
                    }
                    else
                    {
                        // if we are in shutdown do not wait, else wait infinite if send interval is not set
                        millisecondsTillNextSend = _shutdownToken.IsCancellationRequested ? 0 : Timeout.Infinite;
                    }

                    var gotItem = _monitoredPropertiesDataQueue.TryTake(out MessageData messageData,
                        (int) millisecondsTillNextSend, _shutdownToken);

                    if (!gotItem || !(millisecondsTillNextSend <= 0)) continue;
                    if (!IotCentralMode || messageData == null) continue;
                    //get current device twin
                    await hubClient.SendPropertyAsync(messageData, _shutdownToken);
                    SentProperties++;
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, "Error while processing monitored properties.");
                throw;
            }
        }

        public Task MonitoredSettingsProcessor(IHubClient hubClient)
        {
            var nextUpdateTime = DateTime.UtcNow + TimeSpan.FromSeconds(DefaultSendIntervalSeconds);

            try
            {
                while (true)
                {
                    // sanity check the send interval, compute the timeout and get the next monitored item message
                    double millisecondsTillNextUpdate;
                    if (DefaultSendIntervalSeconds > 0)
                    {
                        millisecondsTillNextUpdate = nextUpdateTime.Subtract(DateTime.UtcNow).TotalMilliseconds;
                        if (millisecondsTillNextUpdate < 0)
                        {
                            MissedSettingSendIntervalCount++;
                            // do not wait if we missed the send interval
                            millisecondsTillNextUpdate = 0;
                        }
                    }
                    else
                    {
                        // if we are in shutdown do not wait, else wait infinite if send interval is not set
                        millisecondsTillNextUpdate = _shutdownToken.IsCancellationRequested ? 0 : Timeout.Infinite;
                    }

                    if (!(millisecondsTillNextUpdate <= 0)) continue;
                    var gotItem = _monitoredSettingsDataQueue.TryTake(out var messageData);

                    if (!gotItem) continue;
                    if (!IotCentralMode || messageData == null) continue;
                    if (!HubClient.MonitoredSettingsCollection.TryAdd(
                        messageData.DataChangeMessageData.DisplayName, messageData.DataChangeMessageData.Value))
                    {
                        _logger.Error("Error while storing a new monitored setting.");
                    }

                    SentSettings++;
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, "Error while processing monitored settings.");
                throw;
            }
        }

        public async Task MonitoredIoTCEventsProcessorAsync(IHubClient hubClient, CancellationToken ct)
        {
            uint jsonSquareBracketLength = 2;
            Message tempMsg = new Message();
            // the system properties are MessageId (max 128 byte), Sequence number (ulong), ExpiryTime (DateTime) and more. ideally we get that from the client.
            int systemPropertyLength = 128 + sizeof(ulong) + tempMsg.ExpiryTimeUtc.ToString(CultureInfo.InvariantCulture).Length;
            int applicationPropertyLength = Encoding.UTF8.GetByteCount($"iothub-content-type={CONTENT_TYPE_OPCUAJSON}") + Encoding.UTF8.GetByteCount($"iothub-content-encoding={CONTENT_ENCODING_UTF8}");
            // if batching is requested the buffer will have the requested size, otherwise we reserve the max size
            uint hubMessageBufferSize = (HubMessageSize > 0 ? HubMessageSize : HubMessageSizeMax) - (uint)systemPropertyLength - jsonSquareBracketLength - (uint)applicationPropertyLength;
            byte[] hubMessageBuffer = new byte[hubMessageBufferSize];
            MemoryStream hubMessage = new MemoryStream(hubMessageBuffer);
            DateTime nextSendTime = DateTime.UtcNow + TimeSpan.FromSeconds(DefaultSendIoTcIntervalSeconds);
            bool singleMessageSend = DefaultSendIoTcIntervalSeconds == 0 && HubMessageSize == 0;

            using (hubMessage)
            {
                try
                {
                    string jsonMessage = string.Empty;
                    bool needToBufferMessage = false;
                    int jsonMessageSize = 0;

                    hubMessage.Position = 0;
                    hubMessage.SetLength(0);
                    if (!singleMessageSend)
                    {
                        hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
                    }
                    while (true)
                    {
                        // sanity check the send interval, compute the timeout and get the next monitored item message
                        double millisecondsTillNextSend;
                        if (DefaultSendIoTcIntervalSeconds > 0)
                        {
                            millisecondsTillNextSend = nextSendTime.Subtract(DateTime.UtcNow).TotalMilliseconds;
                            if (millisecondsTillNextSend < 0)
                            {
                                MissedIoTcSendIntervalCount++;
                                // do not wait if we missed the send interval
                                millisecondsTillNextSend = 0;
                            }
                        }
                        else
                        {
                            // if we are in shutdown do not wait, else wait infinite if send interval is not set
                            millisecondsTillNextSend = ct.IsCancellationRequested ? 0 : Timeout.Infinite;
                        }
                        var gotItem = _monitoredIoTcEventDataQueue.TryTake(out MessageData messageData, (int)millisecondsTillNextSend, ct);
                        EventMessageData eventMessageData = messageData?.EventMessageData;

                        // the two commandline parameter --ms (message size) and --si (send interval) control when data is sent to IoTHub/EdgeHub
                        // pls see detailed comments on performance and memory consumption at https://github.com/Azure/iot-edge-opc-publisher

                        // check if we got an item or if we hit the timeout or got canceled
                        if (gotItem)
                        {
                            if (IotCentralMode && eventMessageData != null)
                            {
                                // for IoTCentral we send simple key/value pairs. key is the DisplayName, value the value.
                                jsonMessage = await CreateIoTCentralJsonForEventChangeAsync(eventMessageData)
                                    .ConfigureAwait(false);


                                jsonMessageSize =
                                    Encoding.UTF8.GetByteCount(jsonMessage.ToString(CultureInfo.InvariantCulture));

                                // sanity check that the user has set a large enough messages size
                                if ((HubMessageSize > 0 && jsonMessageSize > HubMessageSize) ||
                                    (HubMessageSize == 0 && jsonMessageSize > hubMessageBufferSize))
                                {
                                    Logger.Error(
                                        $"There is a IoT Central event message (size: {jsonMessageSize}), which will not fit into an hub message (max size: {hubMessageBufferSize}].");
                                    Logger.Error(
                                        $"Please check your hub message size settings. The IoT Central event message will be discarded silently. Sorry:(");
                                    TooLargeCount++;
                                    continue;
                                }

                                // if batching is requested or we need to send at intervals, batch it otherwise send it right away
                                needToBufferMessage = false;
                                if (HubMessageSize > 0 || (HubMessageSize == 0 && DefaultSendIoTcIntervalSeconds > 0))
                                {
                                    // if there is still space to batch, do it. otherwise send the buffer and flag the message for later buffering
                                    if (hubMessage.Position + jsonMessageSize + 1 <= hubMessage.Capacity)
                                    {
                                        // add the message and a comma to the buffer
                                        hubMessage.Write(
                                            Encoding.UTF8.GetBytes(jsonMessage.ToString(CultureInfo.InvariantCulture)),
                                            0, jsonMessageSize);
                                        hubMessage.Write(Encoding.UTF8.GetBytes(","), 0, 1);
                                        Logger.Debug(
                                            $"Added new IoT Central event message with size {jsonMessageSize} to hub message (size is now {(hubMessage.Position - 1)}).");
                                        continue;
                                    }
                                    else
                                    {
                                        needToBufferMessage = true;
                                    }
                                }
                            }
                            else
                            {
                                Logger.Error("Configuration of IoT-Central events is only possible in IoT Central mode");
                            }
                        }
                        else
                        {
                            // if we got no message, we either reached the interval or we are in shutdown and have processed all messages
                            if (ct.IsCancellationRequested)
                            {
                                Logger.Information($"Cancellation requested.");
                                _monitoredItemsDataQueue.CompleteAdding();
                                _monitoredItemsDataQueue.Dispose();
                                break;
                            }
                        }

                        // the batching is completed or we reached the send interval or got a cancelation request
                        try
                        {
                            Microsoft.Azure.Devices.Client.Message encodedhubMessage = null;

                            // if we reached the send interval, but have nothing to send (only the opening square bracket is there), we continue
                            if (!gotItem && hubMessage.Position == 1)
                            {
                                nextSendTime += TimeSpan.FromSeconds(DefaultSendIoTcIntervalSeconds);
                                hubMessage.Position = 0;
                                hubMessage.SetLength(0);
                                if (!singleMessageSend)
                                {
                                    hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
                                }
                                continue;
                            }

                            // if there is no batching and no send interval configured, we send the JSON message we just got, otherwise we send the buffer
                            if (singleMessageSend)
                            {
                                // create the message without brackets
                                encodedhubMessage = new Message(Encoding.UTF8.GetBytes(jsonMessage.ToString(CultureInfo.InvariantCulture)));
                            }
                            else
                            {
                                // remove the trailing comma and add a closing square bracket
                                hubMessage.SetLength(hubMessage.Length - 1);
                                hubMessage.Write(Encoding.UTF8.GetBytes("]"), 0, 1);
                                encodedhubMessage = new Message(hubMessage.ToArray());
                            }
                            if (_hubClient != null)
                            {
                                encodedhubMessage.ContentType = CONTENT_TYPE_OPCUAJSON;
                                encodedhubMessage.ContentEncoding = CONTENT_ENCODING_UTF8;

                                nextSendTime += TimeSpan.FromSeconds(DefaultSendIoTcIntervalSeconds);
                                try
                                {
                                    SentIoTcBytes += encodedhubMessage.GetBytes().Length;
                                    await _hubClient.SendEventAsync(encodedhubMessage).ConfigureAwait(false);
                                    SentIoTcEvents++;
                                    SentLastTime = DateTime.UtcNow;
                                    Logger.Debug($"Sending {encodedhubMessage.BodyStream.Length} bytes to hub.");
                                    Logger.Debug($"Message sent was: {jsonMessage}");
                                }
                                catch
                                {
                                    FailedIoTcMessages++;
                                }

                                // reset the messaage
                                hubMessage.Position = 0;
                                hubMessage.SetLength(0);
                                if (!singleMessageSend)
                                {
                                    hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
                                }

                                // if we had not yet buffered the last message because there was not enough space, buffer it now
                                if (needToBufferMessage)
                                {
                                    // add the message and a comma to the buffer
                                    hubMessage.Write(Encoding.UTF8.GetBytes(jsonMessage.ToString(CultureInfo.InvariantCulture)), 0, jsonMessageSize);
                                    hubMessage.Write(Encoding.UTF8.GetBytes(","), 0, 1);
                                }
                            }
                            else
                            {
                                Logger.Information("No hub client available. Dropping messages...");
                            }
                        }
                        catch (Exception e)
                        {
                            Logger.Error(e, "Exception while sending message to hub. Dropping message...");
                        }
                    }
                }
                catch (Exception e)
                {
                    if (!(e is OperationCanceledException))
                    {
                        Logger.Error(e, "Error while processing monitored item messages.");
                    }
                }
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
            Interlocked.Increment(ref _enqueueCount);
            if (_monitoredIoTcEventDataQueue.TryAdd(message))
                return;
            Interlocked.Increment(ref _enqueueFailureCount);
            if (_enqueueFailureCount % 10000 == 0)
            {
                _logger.Information($"The internal monitored setting message queue is above its capacity of {_monitoredIoTcEventDataQueue.BoundedCapacity}. We have already lost {_enqueueFailureCount} monitored item notifications:(");
            }
        }

        /// <summary>
        /// Creates an IoTCentral JSON message for a event change notification, based on the event configuration for the endpoint.
        /// </summary>
        private async Task<string> CreateIoTCentralJsonForEventChangeAsync(EventMessageData messageData)
        {
            try
            {
                // build the JSON message for IoTCentral
                StringBuilder jsonStringBuilder = new StringBuilder();
                StringWriter jsonStringWriter = new StringWriter(jsonStringBuilder);
                using (JsonWriter jsonWriter = new JsonTextWriter(jsonStringWriter))
                {
                    await jsonWriter.WriteStartObjectAsync(_shutdownToken).ConfigureAwait(false);
                    await jsonWriter.WritePropertyNameAsync(messageData.DisplayName, _shutdownToken).ConfigureAwait(false);
                    var eventValues = string.Join(",", messageData.EventValues.Select(s => new {
                        s.Name,
                        s.Value
                    }));
                    await jsonWriter.WriteValueAsync(eventValues, _shutdownToken).ConfigureAwait(false);
                    await jsonWriter.WriteEndObjectAsync(_shutdownToken).ConfigureAwait(false);
                    await jsonWriter.FlushAsync(_shutdownToken).ConfigureAwait(false);
                }
                return jsonStringBuilder.ToString();
            }
            catch (Exception e)
            {
                Logger.Error(e, "Generation of IoTCentral JSON message failed.");
            }
            return string.Empty;
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
        /// Specifies the send interval in seconds after which a message is sent to the hub.
        /// </summary>
        public static int DefaultSendIoTcIntervalSeconds { get; set; } = 10;
        
        /// <summary>
        /// Number of times we were not able to make the settings send interval, because too high load.
        /// </summary>
        public static long MissedSettingSendIntervalCount { get; set; }

        /// <summary>
        /// Number of times we were not able to make the property send interval, because too high load.
        /// </summary>
        public static long MissedPropertySendIntervalCount { get; set; }
        
        /// <summary>
        /// Number of times we were not able to make the IoTCentral send interval, because too high load.
        /// </summary>
        public static long MissedIoTcSendIntervalCount { get; set; }

        /// <summary>
        /// Number of times we were not able to sent the event message as IoT Central event to the cloud.
        /// </summary>
        public static long FailedIoTcMessages { get; set; }

        /// <summary>
        /// Number of payload bytes we sent to the cloud.
        /// </summary>
        public static long SentIoTcBytes { get; set; }

        /// <summary>
        /// Number of properties we sent to the cloud using deviceTwin
        /// </summary>
        public static long SentProperties { get; set; }

        /// <summary>
        /// Number of settings we sent to the cloud using deviceTwin
        /// </summary>
        public static long SentSettings { get; set; }

        /// <summary>
        /// Number of properties we sent to the cloud using deviceTwin
        /// </summary>
        public static long SentIoTcEvents { get; set; }

        /// <summary>
        /// Specifies the queue capacity for monitored properties.
        /// </summary>
        public static int MonitoredPropertiesQueueCapacity { get; set; } = 8192;

        /// <summary>
        /// Specifies the queue capacity for monitored settings.
        /// </summary>
        public static int MonitoredSettingsQueueCapacity { get; set; } = 8192;

        /// <summary>
        /// Specifies the queue capacity for monitored iot central events.
        /// </summary>
        public static int MonitoredSettingsIoTcEventCapacity { get; set; } = 8192;

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
        private static BlockingCollection<MessageData> _monitoredIoTcEventDataQueue;
        private static Logger _logger;
    }
}
