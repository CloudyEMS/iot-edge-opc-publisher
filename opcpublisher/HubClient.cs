using Microsoft.Azure.Devices.Shared;
using Newtonsoft.Json;
using Opc.Ua;
using opcpublisher.AIT;
using Serilog.Core;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OpcPublisher
{
    using Microsoft.Azure.Devices.Client;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Net;
    using System.Text;


    /// <summary>
    /// Class to encapsulate the IoTHub device/module client interface.
    /// </summary>
    public class HubClient : IHubClient, IDisposable
    {
        /// <summary>
        /// Stores custom product information that will be appended to the user agent string that is sent to IoT Hub.
        /// </summary>
        public string ProductInfo
        {
            get
            {
                if (_iotHubClient == null)
                {
                    return _edgeHubClient.ProductInfo;
                }
                return _iotHubClient.ProductInfo;
            }
            set
            {
                if (_iotHubClient == null)
                {
                    _edgeHubClient.ProductInfo = value;
                    return;
                }
                _iotHubClient.ProductInfo = value;
            }
        }

        public static Dictionary<string, string> MonitoredSettingsCollection { get; private set; }

        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public HubClient(Logger logger)
        {
            MonitoredSettingsCollection = new Dictionary<string, string>();
            _logger = logger;
        }

        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public HubClient(DeviceClient iotHubClient, Logger logger)
        {
            _iotHubClient = iotHubClient;
            MonitoredSettingsCollection = new Dictionary<string, string>();
            _iotHubClient.SetDesiredPropertyUpdateCallbackAsync(HandleSettingChanged, null);
            _logger = logger;
        }

        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public HubClient(ModuleClient edgeHubClient, Logger logger)
        {
            _edgeHubClient = edgeHubClient;
            MonitoredSettingsCollection = new Dictionary<string, string>();
            _edgeHubClient.SetDesiredPropertyUpdateCallbackAsync(HandleSettingChanged, null);
            _logger = logger;
        }

        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public HubClient()
        {
        }

        /// <summary>
        /// Implement IDisposable.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_iotHubClient == null)
                {
                    _edgeHubClient.Dispose();
                    return;
                }
                _iotHubClient.Dispose();
            }
        }

        /// <summary>
        /// Implement IDisposable.
        /// </summary>
        public void Dispose()
        {
            // do cleanup
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Create DeviceClient from the specified connection string using the specified transport type
        /// </summary>
        public static IHubClient CreateDeviceClientFromConnectionString(string connectionString, TransportType transportType, Logger logger)
        {
            return new HubClient(DeviceClient.CreateFromConnectionString(connectionString, transportType), logger);
        }

        /// <summary>
        /// Create ModuleClient from the specified connection string using the specified transport type
        /// </summary>
        public static IHubClient CreateModuleClientFromEnvironment(TransportType transportType, Logger logger)
        {
            return new HubClient(ModuleClient.CreateFromEnvironmentAsync(transportType).Result, logger);
        }

        /// <summary>
        /// Close the client instance
        /// </summary>
        public Task CloseAsync()
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.CloseAsync();
            }
            return _iotHubClient.CloseAsync();
        }

        /// <summary>
        /// Sets the retry policy used in the operation retries.
        /// </summary>
        public void SetRetryPolicy(IRetryPolicy retryPolicy)
        {
            if (_iotHubClient == null)
            {
                _edgeHubClient.SetRetryPolicy(retryPolicy);
                return;
            }
            _iotHubClient.SetRetryPolicy(retryPolicy);
        }

        /// <summary>
        /// Registers a new delegate for the connection status changed callback. If a delegate is already associated, 
        /// it will be replaced with the new delegate.
        /// </summary>
        public void SetConnectionStatusChangesHandler(ConnectionStatusChangesHandler statusChangesHandler)
        {
            if (_iotHubClient == null)
            {
                _edgeHubClient.SetConnectionStatusChangesHandler(statusChangesHandler);
                return;
            }
            _iotHubClient.SetConnectionStatusChangesHandler(statusChangesHandler);
        }

        /// <summary>
        /// Explicitly open the DeviceClient instance.
        /// </summary>
        public Task OpenAsync()
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.OpenAsync();
            }
            return _iotHubClient.OpenAsync();
        }

        /// <summary>
        /// Registers a new delegate for the named method. If a delegate is already associated with
        /// the named method, it will be replaced with the new delegate.
        /// </summary>
        public Task SetMethodHandlerAsync(string methodName, MethodCallback methodHandler)
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.SetMethodHandlerAsync(methodName, methodHandler, _edgeHubClient);
            }
            return _iotHubClient.SetMethodHandlerAsync(methodName, methodHandler, _iotHubClient);
        }

        /// <summary>
        /// Registers a new delegate that is called for a method that doesn't have a delegate registered for its name. 
        /// If a default delegate is already registered it will replace with the new delegate.
        /// </summary>
        public Task SetMethodDefaultHandlerAsync(MethodCallback methodHandler)
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.SetMethodDefaultHandlerAsync(methodHandler, _edgeHubClient);
            }
            return _iotHubClient.SetMethodDefaultHandlerAsync(methodHandler, _iotHubClient);
        }

        /// <summary>
        /// Sends an event to device hub
        /// </summary>
        public Task SendEventAsync(Message message)
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.SendEventAsync(message);
            }
            return _iotHubClient.SendEventAsync(message);
        }

        public Task SendPropertyAsync(MessageData message, CancellationToken ct)
        {
            if (_iotHubClient == null)
            {
                TwinCollection reportedPropertiesEdge = new TwinCollection();
                if (message.EventMessageData != null && message.EventMessageData.EventValues.Count > 0)
                {
                    foreach (var eventValue in message.EventMessageData.EventValues)
                    {
                        if (eventValue.IotCentralEventPublishMode == IotCentralEventPublishMode.Property)
                        {
                            reportedPropertiesEdge[eventValue.Name] = eventValue.Value;
                        }
                    }
                }
                else
                {
                    reportedPropertiesEdge[message.DataChangeMessageData.DisplayName] = message.DataChangeMessageData.Value;
                }

                return _edgeHubClient.UpdateReportedPropertiesAsync(reportedPropertiesEdge, ct);
            }

            TwinCollection reportedProperties = new TwinCollection();
            if (message.EventMessageData != null)
            {
                foreach (var eventValue in message.EventMessageData.EventValues)
                {
                    if (eventValue.IotCentralEventPublishMode == IotCentralEventPublishMode.Property)
                    {
                        reportedProperties[eventValue.Name] = eventValue.Value;
                    }
                }
            }
            else
            {
                reportedProperties[message.DataChangeMessageData.DisplayName] = message.DataChangeMessageData.Value;
            }

            return _iotHubClient.UpdateReportedPropertiesAsync(reportedProperties, ct);
        }

        public Task SendIoTCEventAsync(Message message, CancellationToken ct)
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.SendEventAsync(message);
            }
            return _iotHubClient.SendEventAsync(message);
        }

        private async Task HandleSettingChanged(TwinCollection desiredProperties, object userContext)
        {
            try
            {
                var reportedProperties = new TwinCollection();
                var opcSessions = Program.NodeConfiguration.OpcSessions;
                foreach (var opcSession in opcSessions)
                {
                    foreach (var opcSubscription in opcSession.OpcSubscriptions)
                    {
                        foreach (var opcMonitoredItem in opcSubscription.OpcMonitoredItems)
                        {
                            var key = opcMonitoredItem.DisplayName;
                            if (!MonitoredSettingsCollection.ContainsKey(key) || !desiredProperties.Contains(key))
                            {
                                continue;
                            }
                            //Handle OPC UA Property overwrite and acknowledge setting change
                            //Get JSON Value of desired property which is reported by setting change from IoT Central
                            var jsonValue = new Newtonsoft.Json.Linq.JObject(desiredProperties[key])
                                .GetValue("value").ToString();

                            //Create a new WriteValueCollection to write the new information to OPC UA Server
                            var valuesToWrite = new WriteValueCollection();
                            if (opcMonitoredItem.IdAsNodeId.IdType == IdType.Numeric)
                            {
                                valuesToWrite.Add(
                                   new WriteValue {
                                       NodeId = opcMonitoredItem.ConfigNodeId,
                                       AttributeId = opcMonitoredItem.AttributeId,
                                       Value = new DataValue {
                                           Value = Convert.ToInt32(jsonValue),
                                           ServerTimestamp = DateTime.MinValue,
                                           SourceTimestamp = DateTime.MinValue
                                       }
                                   }
                                );
                            }
                            else
                            {
                                valuesToWrite.Add(
                                   new WriteValue {
                                       NodeId = opcMonitoredItem.ConfigNodeId,
                                       AttributeId = opcMonitoredItem.AttributeId,
                                       Value = new DataValue {
                                           Value = jsonValue,
                                           ServerTimestamp = DateTime.MinValue,
                                           SourceTimestamp = DateTime.MinValue
                                       }
                                   }
                                );
                            }

                            opcSubscription.OpcUaClientSubscription.Subscription.Session.Write(
                                null,
                                valuesToWrite,
                                out var results,
                                out var diagnosticInfos);
                            ClientBase.ValidateResponse(results, valuesToWrite);
                            ClientBase.ValidateDiagnosticInfos(diagnosticInfos, valuesToWrite);

                            string status,
                                message;
                            if (StatusCode.IsBad(results[0]))
                            {
                                _logger.Error($"[{results[0].ToString()}]: Cannot write Setting value of Monitored Item with NodeId {opcMonitoredItem.Id} and DisplayName {opcMonitoredItem.DisplayName}");
                                status = "error";
                                message = $"Failure during synchronizing OPC UA Values, Reason: {results[0].ToString()}";
                            }
                            else
                            {
                                status = "completed";
                                message = "Processed";
                            }

                            reportedProperties[key] = new {
                                value = desiredProperties[key]["value"],
                                status,
                                desiredVersion = desiredProperties["$version"],
                                message
                            };
                            await updateReportedPropertiesAsync(reportedProperties);
                        }
                    }
                }
            }

            catch (Exception e)
            {
                _logger.Error(e, "Error while updating reported Setting.");
            }
        }

        public async Task<MethodResponse> DefaultCommandHandlerAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "DefaultCommandHandlerAsync:";
            string message = $"Method '{methodRequest.Name}' successfully received. Started to handle Command.";
            _logger.Information($"{logPrefix} {message}");
            string resultString = null;
            HttpStatusCode resultStatusCode = HttpStatusCode.NoContent;

            var opcSessions = Program.NodeConfiguration.OpcSessions;
            foreach (var opcSession in opcSessions)
            {
                foreach (var opcSubscription in opcSession.OpcSubscriptions)
                {
                    foreach (var opcMonitoredItem in opcSubscription.OpcMonitoredItems)
                    {
                        if(opcMonitoredItem.DisplayName == methodRequest.Name)
                        {
                            List<object> inputArguments = new List<object>();
                            var commandParametersJObject = JObject.Parse(methodRequest.DataAsJson)["commandParameters"];
                            var parameterDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(commandParametersJObject.ToString());
                            var session = opcSession.OpcUaClientSession.GetSession();

                            var references = session.FetchReferences(opcMonitoredItem.ConfigNodeId);
                            var nodeValue = session.ReadValue(references.Where(w => w.DisplayName == "InputArguments").SingleOrDefault().NodeId.ToString());

                            foreach (var param in parameterDictionary)
                            {
                                foreach (var inputArgument in ((Opc.Ua.ExtensionObject[])nodeValue.Value))
                                {
                                    var opcUaArgument = (Argument)inputArgument.Body;
                                    if (param.Key != opcUaArgument.Name)
                                        continue;
                                    //Int datatypes
                                    if (opcUaArgument.DataType == DataTypeIds.UInt16)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToUInt16(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.UInt32)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToUInt32(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.UInt64)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToUInt64(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.Int16)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToInt16(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.Int32)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToInt32(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.Int64)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToInt64(param.Value)));
                                    }
                                    //String datatypes
                                    else if (opcUaArgument.DataType == DataTypeIds.String || 
                                        opcUaArgument.DataType == DataTypeIds.LocalizedText ||
                                        opcUaArgument.DataType == DataTypeIds.XmlElement ||
                                        opcUaArgument.DataType == DataTypeIds.QualifiedName ||
                                        opcUaArgument.DataType == DataTypeIds.DateTime
                                        )
                                    {
                                        inputArguments.Add(new Variant(param.Value));
                                    }
                                    //Enum types based on EnumIndex
                                    else if(opcUaArgument.DataType == DataTypeIds.ServerState ||
                                        opcUaArgument.DataType == DataTypeIds.RedundancySupport ||
                                        opcUaArgument.DataType == DataTypeIds.NamingRuleType ||
                                        opcUaArgument.DataType == DataTypeIds.IdType ||
                                        opcUaArgument.DataType == DataTypeIds.NodeClass
                                        )
                                    {
                                        inputArguments.Add(new Variant(Convert.ToInt32(param.Value)));
                                    }
                                    //Default -> normal string
                                    else
                                    {
                                        var errorMessage = $"{logPrefix}: DataType {opcUaArgument.DataType.ToString()} is not implemented as input parameter yet. " +
                                            $"It will be added as normal string, if your method call fails it is currently not supported.";
                                        _logger.Error(errorMessage);
                                        resultString = errorMessage;
                                        resultStatusCode = HttpStatusCode.NotImplemented;
                                        inputArguments.Add(new Variant(param.Value));
                                    }
                                }
                            }
                            try
                            {
                                opcSubscription.OpcUaClientSubscription.Subscription.Session.Call("ns=0;i=2253", opcMonitoredItem.ConfigNodeId, inputArguments.ToArray());
                                if (string.IsNullOrEmpty(resultString))
                                {
                                    resultString = $"Successfully executed method {methodRequest.Name}";
                                    resultStatusCode = HttpStatusCode.OK;
                                }
                            }
                            catch(Exception ex)
                            {
                                resultString = ex.Message;
                                resultStatusCode = HttpStatusCode.InternalServerError;
                            }
                        }
                    }
                }
            }

            //Response messages in IoT Central must be sent via reported properties
            var reportedProperties = new TwinCollection();
            reportedProperties["GetMonitoredItems"] = new {
                value = $"Response - HTTP{resultStatusCode}: " + resultString
            };
            await updateReportedPropertiesAsync(reportedProperties);

            var responseJson = JsonConvert.SerializeObject("Response:" + resultString);
            byte[] result = Encoding.UTF8.GetBytes(responseJson);
            MethodResponse methodResponse = new MethodResponse(result, (int)resultStatusCode);
            return methodResponse;
        }

        private async Task updateReportedPropertiesAsync(TwinCollection reportedProperties)
        {
            if (_iotHubClient == null)
            {
                await _edgeHubClient.UpdateReportedPropertiesAsync(reportedProperties);
            }
            else
            {
                await _iotHubClient.UpdateReportedPropertiesAsync(reportedProperties);
            }
        }

        private readonly Logger _logger;
        private static DeviceClient _iotHubClient;
        private static ModuleClient _edgeHubClient;
    }
}
