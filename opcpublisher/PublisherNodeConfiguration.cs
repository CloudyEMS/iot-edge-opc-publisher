﻿using Newtonsoft.Json;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace OpcPublisher
{
    using OpcPublisher.Crypto;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Web;
    using static OpcApplicationConfiguration;
    using static OpcMonitoredItem;
    using static OpcSession;
    using static Program;

    public class PublisherNodeConfiguration : IPublisherNodeConfiguration, IDisposable
    {
        /// <summary>
        /// Name of the node configuration file.
        /// </summary>
        public static string PublisherNodeConfigurationFilename { get; set; } = $"{System.IO.Directory.GetCurrentDirectory()}{Path.DirectorySeparatorChar}publishednodes.json";

        /// <summary>
        /// Number of configured OPC UA sessions.
        /// </summary>
        public int NumberOfOpcSessionsConfigured
        {
            get
            {
                int result = 0;
                try
                {
                    OpcSessionsListSemaphore.Wait();
                    result = OpcSessions.Count();
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                return result;
            }
        }

        /// <summary>
        /// Number of connected OPC UA session.
        /// </summary>
        public int NumberOfOpcSessionsConnected
        {
            get
            {
                int result = 0;
                try
                {
                    OpcSessionsListSemaphore.Wait();
                    result = OpcSessions.Count(s => s.State == OpcSession.SessionState.Connected);
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                return result;
            }
        }

        /// <summary>
        /// Number of configured OPC UA subscriptions.
        /// </summary>
        public int NumberOfOpcSubscriptionsConfigured
        {
            get
            {
                int result = 0;
                try
                {
                    OpcSessionsListSemaphore.Wait();
                    foreach (var opcSession in OpcSessions)
                    {
                        result += opcSession.GetNumberOfOpcSubscriptions();
                    }
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                return result;
            }
        }

        /// <summary>
        /// Number of connected OPC UA subscriptions.
        /// </summary>
        public int NumberOfOpcSubscriptionsConnected
        {
            get
            {
                int result = 0;
                try
                {
                    OpcSessionsListSemaphore.Wait();
                    var opcSessions = OpcSessions.Where(s => s.State == OpcSession.SessionState.Connected);
                    foreach (var opcSession in opcSessions)
                    {
                        result += opcSession.GetNumberOfOpcSubscriptions();
                    }
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                return result;
            }
        }

        /// <summary>
        /// Number of data change monitored items configured.
        /// </summary>
        public int NumberOfOpcDataChangeMonitoredItemsConfigured
        {
            get
            {
                int result = 0;
                try
                {
                    OpcSessionsListSemaphore.Wait();
                    foreach (var opcSession in OpcSessions)
                    {
                        result += opcSession.GetNumberOfOpcDataChangeMonitoredItemsConfigured();
                    }
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                return result;
            }
        }

        /// <summary>
        /// Number of data change monitored items monitored.
        /// </summary>
        public int NumberOfOpcDataChangeMonitoredItemsMonitored
        {
            get
            {
                int result = 0;
                try
                {
                    OpcSessionsListSemaphore.Wait();
                    var opcSessions = OpcSessions.Where(s => s.State == OpcSession.SessionState.Connected);
                    foreach (var opcSession in opcSessions)
                    {
                        result += opcSession.GetNumberOfOpcDataChangeMonitoredItemsMonitored();
                    }
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                return result;
            }
        }

        /// <summary>
        /// Number of data change monitored items to be removed.
        /// </summary>
        public int NumberOfOpcDataChangeMonitoredItemsToRemove
        {
            get
            {
                int result = 0;
                try
                {
                    OpcSessionsListSemaphore.Wait();
                    foreach (var opcSession in OpcSessions)
                    {
                        result += opcSession.GetNumberOfOpcDataChangeMonitoredItemsToRemove();
                    }
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                return result;
            }
        }

        /// <summary>
        /// Number of event monitored items configured.
        /// </summary>
        public int NumberOfOpcEventMonitoredItemsConfigured
        {
            get
            {
                int result = 0;
                try
                {
                    OpcSessionsListSemaphore.Wait();
                    foreach (var opcSession in OpcSessions)
                    {
                        result += opcSession.GetNumberOfOpcEventMonitoredItemsConfigured();
                    }
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                return result;
            }
        }

        /// <summary>
        /// Number of event monitored items monitored.
        /// </summary>
        public int NumberOfOpcEventMonitoredItemsMonitored
        {
            get
            {
                int result = 0;
                try
                {
                    OpcSessionsListSemaphore.Wait();
                    var opcSessions = OpcSessions.Where(s => s.State == OpcSession.SessionState.Connected);
                    foreach (var opcSession in opcSessions)
                    {
                        result += opcSession.GetNumberOfOpcEventMonitoredItemsMonitored();
                    }
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                return result;
            }
        }

        /// <summary>
        /// Number of event monitored items to be removed.
        /// </summary>
        public int NumberOfOpcEventMonitoredItemsToRemove
        {
            get
            {
                int result = 0;
                try
                {
                    OpcSessionsListSemaphore.Wait();
                    foreach (var opcSession in OpcSessions)
                    {
                        result += opcSession.GetNumberOfOpcEventMonitoredItemsToRemove();
                    }
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                return result;
            }
        }

        /// <summary>
        /// Semaphore to protect the node configuration data structures.
        /// </summary>
        public SemaphoreSlim PublisherNodeConfigurationSemaphore { get; set; }

        /// <summary>
        /// Semaphore to protect the node configuration file.
        /// </summary>
        public SemaphoreSlim PublisherNodeConfigurationFileSemaphore { get; set; }

        /// <summary>
        /// Semaphore to protect the OPC UA sessions list.
        /// </summary>
        public SemaphoreSlim OpcSessionsListSemaphore { get; set; }

#pragma warning disable CA2227 // Collection properties should be read only

        /// <summary>
        /// List of configured OPC UA sessions.
        /// </summary>
        public virtual List<IOpcSession> OpcSessions { get; set; } = new List<IOpcSession>();
#pragma warning restore CA2227 // Collection properties should be read only

        /// <summary>
        /// Get the singleton.
        /// </summary>
        public static IPublisherNodeConfiguration Instance
        {
            get
            {
                lock (_singletonLock)
                {
                    if (_instance == null)
                    {
                        _instance = new PublisherNodeConfiguration();
                    }
                    return _instance;
                }
            }
        }

        /// <summary>
        /// Ctor to initialize resources for the telemetry configuration.
        /// </summary>
        public PublisherNodeConfiguration()
        {
            OpcSessionsListSemaphore = new SemaphoreSlim(1);
            PublisherNodeConfigurationSemaphore = new SemaphoreSlim(1);
            PublisherNodeConfigurationFileSemaphore = new SemaphoreSlim(1);
            OpcSessions.Clear();
            _nodePublishingConfiguration = new List<NodePublishingConfigurationModel>();
            _eventConfiguration = new List<EventConfigurationModel>();
            _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();

            // read the configuration from the configuration file
            if (!ReadConfigAsync().Result)
            {
                string errorMessage = $"Error while reading the node configuration file '{PublisherNodeConfigurationFilename}'";
                Logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }

            // create the configuration data structures
            if (!CreateOpcPublishingDataAsync().Result)
            {
                string errorMessage = $"Error while creating node configuration data structures.";
                Logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
        }

        /// <summary>
        /// Implement IDisposable.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                OpcSessionsListSemaphore.Wait();
                foreach (var opcSession in OpcSessions)
                {
                    opcSession.Dispose();
                }
                OpcSessions?.Clear();
                OpcSessionsListSemaphore?.Dispose();
                OpcSessionsListSemaphore = null;
                PublisherNodeConfigurationSemaphore?.Dispose();
                PublisherNodeConfigurationSemaphore = null;
                PublisherNodeConfigurationFileSemaphore?.Dispose();
                PublisherNodeConfigurationFileSemaphore = null;
                _nodePublishingConfiguration?.Clear();
                _nodePublishingConfiguration = null;
                _eventConfiguration?.Clear();
                _eventConfiguration = null;
                lock (_singletonLock)
                {
                    _instance = null;
                }
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
        /// Initialize the node configuration.
        /// </summary>
        /// <returns></returns>
        public async Task InitAsync()
        {
            // Shutdown all old sessions
            while (OpcSessions.Count > 0)
            {
                IOpcSession opcSession = null;
                try
                {
                    await OpcSessionsListSemaphore.WaitAsync().ConfigureAwait(false);
                    opcSession = OpcSessions.ElementAt(0);
                    OpcSessions.RemoveAt(0);
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
                await (opcSession?.ShutdownAsync()).ConfigureAwait(false);
            }

            // reset data
            _eventConfiguration.Clear();
            _configurationFileEntries.Clear();
            _nodePublishingConfiguration.Clear();

            // read the configuration from the configuration file
            if (!await ReadConfigAsync().ConfigureAwait(false))
            {
                string errorMessage = $"Error while reading the node configuration file '{PublisherNodeConfigurationFilename}'";
                Logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }

            // create the configuration data structures
            if (!await CreateOpcPublishingDataAsync().ConfigureAwait(false))
            {
                string errorMessage = $"Error while creating node configuration data structures.";
                Logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
        }

        /// <summary>
        /// Read and parse the publisher node configuration file.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> ReadConfigAsync()
        {
            // get information on the nodes to publish and validate the json by deserializing it.
            try
            {
                await PublisherNodeConfigurationSemaphore.WaitAsync().ConfigureAwait(false);
                if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("_GW_PNFP")))
                {
                    Logger.Information("Publishing node configuration file path read from environment.");
                    PublisherNodeConfigurationFilename = Environment.GetEnvironmentVariable("_GW_PNFP");
                }
                Logger.Information($"The name of the configuration file for published nodes is: {PublisherNodeConfigurationFilename}");

                // if the file exists, read it, if not just continue
                if (File.Exists(PublisherNodeConfigurationFilename))
                {
                    Logger.Information($"Attemtping to load node configuration from: {PublisherNodeConfigurationFilename}");
                    try
                    {
                        await PublisherNodeConfigurationFileSemaphore.WaitAsync().ConfigureAwait(false);
                        var json = File.ReadAllText(PublisherNodeConfigurationFilename);
                        _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(json);
                    }
                    finally
                    {
                        PublisherNodeConfigurationFileSemaphore.Release();
                    }

                    if (_configurationFileEntries != null)
                    {
                        Logger.Information($"Loaded {_configurationFileEntries.Count} config file entry/entries.");
                        foreach (var publisherConfigFileEntryLegacy in _configurationFileEntries)
                        {
                            if (publisherConfigFileEntryLegacy.NodeId == null)
                            {
                                // process node configuration
                                foreach (var opcNode in publisherConfigFileEntryLegacy.OpcNodes)
                                {
                                    if (opcNode.ExpandedNodeId != null)
                                    {
                                        ExpandedNodeId expandedNodeId = ExpandedNodeId.Parse(opcNode.ExpandedNodeId);
                                        _nodePublishingConfiguration.Add(new NodePublishingConfigurationModel(expandedNodeId, opcNode.ExpandedNodeId,
                                            publisherConfigFileEntryLegacy.EndpointId, publisherConfigFileEntryLegacy.EndpointName, publisherConfigFileEntryLegacy.EndpointUrl.OriginalString,
                                            publisherConfigFileEntryLegacy.UseSecurity,
                                            opcNode.OpcPublishingInterval, opcNode.OpcSamplingInterval, opcNode.Key, opcNode.DisplayName,
                                            opcNode.HeartbeatInterval, opcNode.SkipFirst, publisherConfigFileEntryLegacy.OpcAuthenticationMode,
                                            publisherConfigFileEntryLegacy.EncryptedAuthCredential, opcNode.IotCentralItemPublishMode));
                                    }
                                    else
                                    {
                                        // check Id string to check which format we have
                                        if (opcNode.Id.StartsWith("nsu=", StringComparison.InvariantCulture))
                                        {
                                            // ExpandedNodeId format
                                            ExpandedNodeId expandedNodeId = ExpandedNodeId.Parse(opcNode.Id);
                                            _nodePublishingConfiguration.Add(new NodePublishingConfigurationModel(expandedNodeId, opcNode.Id,
                                                publisherConfigFileEntryLegacy.EndpointId, publisherConfigFileEntryLegacy.EndpointName, publisherConfigFileEntryLegacy.EndpointUrl.OriginalString,
                                                publisherConfigFileEntryLegacy.UseSecurity,
                                                opcNode.OpcPublishingInterval, opcNode.OpcSamplingInterval, opcNode.Key, opcNode.DisplayName,
                                                opcNode.HeartbeatInterval, opcNode.SkipFirst, publisherConfigFileEntryLegacy.OpcAuthenticationMode,
                                                publisherConfigFileEntryLegacy.EncryptedAuthCredential, opcNode.IotCentralItemPublishMode));
                                        }
                                        else
                                        {
                                            // NodeId format
                                            NodeId nodeId = NodeId.Parse(opcNode.Id);
                                            _nodePublishingConfiguration.Add(new NodePublishingConfigurationModel(nodeId, opcNode.Id,
                                                publisherConfigFileEntryLegacy.EndpointId, publisherConfigFileEntryLegacy.EndpointName, publisherConfigFileEntryLegacy.EndpointUrl.OriginalString,
                                                publisherConfigFileEntryLegacy.UseSecurity,
                                                opcNode.OpcPublishingInterval, opcNode.OpcSamplingInterval, opcNode.Key, opcNode.DisplayName,
                                                opcNode.HeartbeatInterval, opcNode.SkipFirst, publisherConfigFileEntryLegacy.OpcAuthenticationMode,
                                                publisherConfigFileEntryLegacy.EncryptedAuthCredential, opcNode.IotCentralItemPublishMode));
                                        }
                                    }
                                }

                                // process event configuration
                                foreach (var opcEvent in publisherConfigFileEntryLegacy.OpcEvents)
                                {
                                    _eventConfiguration.Add(
                                        new EventConfigurationModel(
                                            publisherConfigFileEntryLegacy.EndpointId.ToString(),
                                            publisherConfigFileEntryLegacy.EndpointName,
                                            publisherConfigFileEntryLegacy.EndpointUrl.OriginalString,
                                            publisherConfigFileEntryLegacy.UseSecurity,
                                            publisherConfigFileEntryLegacy.OpcAuthenticationMode,
                                            publisherConfigFileEntryLegacy.EncryptedAuthCredential,
                                            opcEvent.Id,
                                            opcEvent.DisplayName,
                                            opcEvent.SelectClauses,
                                            opcEvent.WhereClause,
                                            opcEvent.IotCentralEventPublishMode));
                                }
                            }
                            else
                            {
                                // TODO SER Check if the legacy support can be removed
                                // NodeId (ns=) format node configuration syntax using default sampling and publishing interval.
                                _nodePublishingConfiguration.Add(new NodePublishingConfigurationModel(
                                    publisherConfigFileEntryLegacy.NodeId,
                                    publisherConfigFileEntryLegacy.NodeId.ToString(),
                                    publisherConfigFileEntryLegacy.EndpointId,
                                    publisherConfigFileEntryLegacy.EndpointName,
                                    publisherConfigFileEntryLegacy.EndpointUrl.OriginalString,
                                    publisherConfigFileEntryLegacy.UseSecurity,
                                    null, null, publisherConfigFileEntryLegacy.NodeId.ToString(), null,
                                    null, null, publisherConfigFileEntryLegacy.OpcAuthenticationMode, publisherConfigFileEntryLegacy.EncryptedAuthCredential, null));
                            }
                        }
                    }
                }
                else
                {
                    Logger.Information($"The node configuration file '{PublisherNodeConfigurationFilename}' does not exist. Continue and wait for remote configuration requests.");
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, "Loading of the node configuration file failed. Does the file exist and has correct syntax? Exiting...");
                return false;
            }
            finally
            {
                PublisherNodeConfigurationSemaphore.Release();
            }
            Logger.Information($"There are {_nodePublishingConfiguration.Count.ToString(CultureInfo.InvariantCulture)} nodes to publish.");
            Logger.Information($"There are {_eventConfiguration.Count.ToString(CultureInfo.InvariantCulture)} events to publish.");
            return true;
        }

        public async Task<string> ReadConfigAsyncAsJson()
        {
            // get information on the nodes to publish and validate the json by deserializing it.
            try
            {
                await PublisherNodeConfigurationSemaphore.WaitAsync().ConfigureAwait(false);
                if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("_GW_PNFP")))
                {
                    Logger.Information("Publishing node configuration file path read from environment.");
                    PublisherNodeConfigurationFilename = Environment.GetEnvironmentVariable("_GW_PNFP");
                }

                Logger.Information($"The name of the configuration file for published nodes is: {PublisherNodeConfigurationFilename}");

                // if the file exists, read it, if not just continue
                if (File.Exists(PublisherNodeConfigurationFilename))
                {
                    Logger.Information($"Attempting to load node configuration from: {PublisherNodeConfigurationFilename}");
                    try
                    {
                        await PublisherNodeConfigurationFileSemaphore.WaitAsync().ConfigureAwait(false);
                        var json = File.ReadAllText(PublisherNodeConfigurationFilename);
                        _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(json);
                        return json;
                    }
                    finally
                    {
                        PublisherNodeConfigurationFileSemaphore.Release();
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, "Loading of the node configuration file failed. Does the file exist and has correct syntax? Exiting...");
            }
            finally
            {
                PublisherNodeConfigurationSemaphore.Release();
            }
            return null;
        }

        public async Task<bool> SaveJsonAsPublisherNodeConfiguration(string json)
        {
            // get information on the nodes to publish and validate the json by deserializing it.
            try
            {
                await PublisherNodeConfigurationSemaphore.WaitAsync().ConfigureAwait(false);
                if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("_GW_PNFP")))
                {
                    Logger.Information("Publishing node configuration file path read from environment.");
                    PublisherNodeConfigurationFilename = Environment.GetEnvironmentVariable("_GW_PNFP");
                }

                Logger.Information($"The name of the configuration file for published nodes is: {PublisherNodeConfigurationFilename}");

                // if the file exists, read it, if not just continue
                if (File.Exists(PublisherNodeConfigurationFilename))
                {
                    Logger.Information($"Attempting to parse node configuration from input");
                    try
                    {
                        await PublisherNodeConfigurationFileSemaphore.WaitAsync().ConfigureAwait(false);
                        //try to parse in needed datastructure
                        _configurationFileEntries =
                            JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(json);
                        Logger.Information($"JSON received is: {json}");
                        File.WriteAllText(PublisherNodeConfigurationFilename, JsonConvert.SerializeObject(_configurationFileEntries));
                        return true;
                    }
                    catch (Exception e)
                    {
                        Logger.Error(e.Message);
                    }
                    finally
                    {
                        PublisherNodeConfigurationFileSemaphore.Release();
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, "Loading of the node configuration file failed. Does the file exist and has correct syntax? Exiting...");
            }
            finally
            {
                PublisherNodeConfigurationSemaphore.Release();
            }
            return false;
        }

        public virtual IOpcSession CreateOpcSession(Guid endpointId, string endpointName, string endpointUrl, bool useSecurity, uint sessionTimeout, OpcAuthenticationMode opcAuthenticationMode, EncryptedNetworkCredential encryptedAuthCredential)
        {
            // I don't know why this argument is overridden, but now it
            // could be a node or event publishing configuration
            var useSecurityOverride =
                _nodePublishingConfiguration.FirstOrDefault(n => n.EndpointId == endpointId)?.UseSecurity ??
                _eventConfiguration.FirstOrDefault(n => n.EndpointId == endpointId.ToString())?.UseSecurity;

            return new OpcSession(
                endpointId,
                endpointName,
                endpointUrl,
                useSecurityOverride ?? throw new InvalidOperationException("No configuration for endpoint found"),
                OpcSessionCreationTimeout,
                opcAuthenticationMode,
                encryptedAuthCredential);
        }

        /// <summary>
        /// Create the publisher data structures to manage OPC sessions, subscriptions and monitored items.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> CreateOpcPublishingDataAsync()
        {
            // create a list to manage sessions, subscriptions and monitored items.
            try
            {
                await PublisherNodeConfigurationSemaphore.WaitAsync().ConfigureAwait(false);
                await OpcSessionsListSemaphore.WaitAsync().ConfigureAwait(false);

                // create data for data change configuration
                var uniqueNodesEndpointIds = _nodePublishingConfiguration.Select(n => n.EndpointId).Distinct();
                foreach (var endpointId in uniqueNodesEndpointIds)
                {
                    var currentNodePublishingConfiguration = _nodePublishingConfiguration.Where(n => n.EndpointId == endpointId).First();

                    EncryptedNetworkCredential encryptedAuthCredential = null;

                    if (currentNodePublishingConfiguration.OpcAuthenticationMode == OpcAuthenticationMode.UsernamePassword)
                    {
                        if (currentNodePublishingConfiguration.EncryptedAuthCredential == null)
                        {
                            throw new NullReferenceException($"Could not retrieve credentials to authenticate to the server. Please check if 'OpcAuthenticationUsername' and 'OpcAuthenticationPassword' are set in configuration.");
                        }

                        encryptedAuthCredential = currentNodePublishingConfiguration.EncryptedAuthCredential;
                    }

                    // create new session info.
                    IOpcSession opcSession = new OpcSession(currentNodePublishingConfiguration.EndpointId, currentNodePublishingConfiguration.EndpointName, currentNodePublishingConfiguration.EndpointUrl, currentNodePublishingConfiguration.UseSecurity, OpcSessionCreationTimeout, currentNodePublishingConfiguration.OpcAuthenticationMode, encryptedAuthCredential);

                    // create a subscription for each distinct publishing inverval
                    var nodesDistinctPublishingInterval = _nodePublishingConfiguration.Where(n => n.EndpointId.Equals(currentNodePublishingConfiguration.EndpointId)).Select(c => c.OpcPublishingInterval).Distinct();
                    foreach (var nodeDistinctPublishingInterval in nodesDistinctPublishingInterval)
                    {
                        // create a subscription for the publishing interval and add it to the session.
                        IOpcSubscription opcSubscription = new OpcSubscription(nodeDistinctPublishingInterval);

                        // add all nodes with this OPC publishing interval to this subscription.
                        var nodesWithSamePublishingInterval = _nodePublishingConfiguration.Where(n => n.EndpointId.Equals(currentNodePublishingConfiguration.EndpointId)).Where(n => n.OpcPublishingInterval == nodeDistinctPublishingInterval);
                        foreach (var nodeInfo in nodesWithSamePublishingInterval)
                        {
                            // differentiate if NodeId or ExpandedNodeId format is used
                            if (nodeInfo.ExpandedNodeId != null)
                            {
                                // create a monitored item for the node, we do not have the namespace index without a connected session.
                                // so request a namespace update.
                                OpcMonitoredItem opcMonitoredItem = new OpcMonitoredItem(nodeInfo.ExpandedNodeId,
                                    opcSession.EndpointId,
                                    opcSession.EndpointUrl,
                                    nodeInfo.OpcSamplingInterval, nodeInfo.Key, nodeInfo.DisplayName, nodeInfo.HeartbeatInterval,
                                    nodeInfo.SkipFirst, nodeInfo.IotCentralItemPublishMode);
                                opcSubscription.OpcMonitoredItems.Add(opcMonitoredItem);
                                Interlocked.Increment(ref NodeConfigVersion);
                            }
                            else if (nodeInfo.NodeId != null)
                            {
                                // create a monitored item for the node with the configured or default sampling interval
                                OpcMonitoredItem opcMonitoredItem = new OpcMonitoredItem(nodeInfo.NodeId,
                                    opcSession.EndpointId,
                                    opcSession.EndpointUrl,
                                    nodeInfo.OpcSamplingInterval, nodeInfo.Key, nodeInfo.DisplayName, nodeInfo.HeartbeatInterval,
                                    nodeInfo.SkipFirst, nodeInfo.IotCentralItemPublishMode);
                                opcSubscription.OpcMonitoredItems.Add(opcMonitoredItem);
                                Interlocked.Increment(ref NodeConfigVersion);
                            }
                            else
                            {
                                Logger.Error($"Node {nodeInfo.Id} has an invalid format. Skipping...");
                            }
                        }

                        // add subscription to session.
                        opcSession.OpcSubscriptions.Add(opcSubscription);
                    }

                    // add session
                    OpcSessions.Add(opcSession);
                }

                // create data for event configuration
                var uniqueEventsEndpointIds = _eventConfiguration.Select(n => new Guid(n.EndpointId)).Distinct();
                foreach (var endpointId in uniqueEventsEndpointIds)
                {
                    var eventConfiguration = _eventConfiguration.First(n => n.EndpointId == endpointId.ToString());

                    EncryptedNetworkCredential encryptedAuthCredential = null;

                    if (eventConfiguration.OpcAuthenticationMode == OpcAuthenticationMode.UsernamePassword)
                    {
                        if (eventConfiguration.EncryptedAuthCredential == null)
                        {
                            throw new NullReferenceException($"Could not retrieve credentials to authenticate to the server. Please check if 'OpcAuthenticationUsername' and 'OpcAuthenticationPassword' are set in configuration.");
                        }

                        encryptedAuthCredential = eventConfiguration.EncryptedAuthCredential;
                    }

                    bool addSession = false;

                    // create new session info, if needed
                    IOpcSession opcSession = OpcSessions.Find(s => s.EndpointId == endpointId);
                    if (opcSession == null)
                    {
                        var eventConfig = _eventConfiguration.Where(n => n.EndpointId == endpointId.ToString()).First();
                        opcSession = new OpcSession(endpointId, eventConfig.EndpointName, eventConfig.EndpointUrl, eventConfig.UseSecurity, OpcSessionCreationTimeout, eventConfiguration.OpcAuthenticationMode, encryptedAuthCredential);
                        addSession = true;
                    }

                    // create a subscription for each event source
                    var distinctEventSources = _eventConfiguration.Where(n => n.EndpointId.Equals(endpointId.ToString())).Select(c => c.Id).Distinct();
                    foreach (var distinctEventSource in distinctEventSources)
                    {
                        // create a subscription for the event source and add it to ´the session
                        IOpcSubscription opcSubscription = new OpcSubscription(distinctEventSource);

                        // add all event subscriptions for this event source in the subscription.
                        var eventsWithTheSameSource = _eventConfiguration.Where(n => n.EndpointId.Equals(endpointId.ToString())).Where(n => n.Id == distinctEventSource);
                        foreach (var opcEvent in eventsWithTheSameSource)
                        {
                            if (opcEvent.SelectClauses == null || opcEvent.SelectClauses.Count == 0)
                            {
                                string errorMessage = $"An event configuration needs to have at least one SelectClause. The configuration for EndpointId '{opcEvent.EndpointId}' and Id '{opcEvent.Id}' has none.";
                                Logger.Error(errorMessage);
                                throw new Exception(errorMessage);
                            }
                            OpcMonitoredItem opcMonitoredItem = new OpcMonitoredItem(opcEvent, opcSession.EndpointId, opcSession.EndpointUrl);
                            opcSubscription.OpcMonitoredItems.Add(opcMonitoredItem);
                            Interlocked.Increment(ref NodeConfigVersion);
                        }

                        // add event subscription to session.
                        opcSession.OpcEventSubscriptions.Add(opcSubscription);
                    }

                    // add session
                    if (addSession)
                    {
                        OpcSessions.Add(opcSession);
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, "Creation of the internal OPC data managment structures failed. Exiting...");
                return false;
            }
            finally
            {
                OpcSessionsListSemaphore.Release();
                PublisherNodeConfigurationSemaphore.Release();
            }
            // dump node configuration
            if (LogLevel == "debug")
            {
                foreach (var opcSession in OpcSessions)
                {
                    Logger.Debug($"Session to endpoint '{opcSession.EndpointId}': '{opcSession.EndpointName}' (URL '{opcSession.EndpointUrl}'), use security: {opcSession.UseSecurity}");
                    foreach (var opcSubscription in opcSession.OpcSubscriptions)
                    {
                        Logger.Debug($"  Susbscription for DataChange with requested PublishingInterval '{opcSubscription.RequestedPublishingInterval}'");
                        foreach (var opcMonitoredItem in opcSubscription.OpcMonitoredItems)
                        {
                            Logger.Debug($"    Node to monitor '{opcMonitoredItem.ConfigNodeId ?? opcMonitoredItem.ConfigExpandedNodeId ?? opcMonitoredItem.Id}' with requested SamplingInterval {opcMonitoredItem.RequestedSamplingInterval}");
                        }
                    }
                    foreach (var opcSubscription in opcSession.OpcEventSubscriptions)
                    {
                        Logger.Debug($"  Susbscription for Events");
                        foreach (var opcMonitoredItem in opcSubscription.OpcMonitoredItems)
                        {
                            Logger.Debug($"    Event notifier to monitor '{opcMonitoredItem.Id}'");
                            int i = 0;
                            foreach (var selectClause in opcMonitoredItem.EventConfiguration.SelectClauses)
                            {
                                Logger.Debug($"      SelectClause {i++}:");
                                Logger.Debug($"        From TypeId: '{selectClause.TypeId}' select field with browse path '{string.Join(", ", selectClause.BrowsePaths.ToArray())}'");
                            }
                            i = 0;
                            foreach (var whereClauseElement in opcMonitoredItem.EventConfiguration.WhereClause)
                            {
                                Logger.Debug($"      WhereClauseElement {i++}: '{whereClauseElement.Operator}'");
                                Logger.Debug($"        Operator: '{whereClauseElement.Operator}'");
                                int j = 0;
                                foreach (var operand in whereClauseElement.Operands)
                                {
                                    if (operand.Element != null)
                                    {
                                        Logger.Debug($"        Operand {j++}(Element): '{operand.Element}'");
                                    }
                                    if (operand.Literal != null)
                                    {
                                        Logger.Debug($"        Operand {j++}(Literal): '{operand.Literal}'");
                                    }
                                    if (operand.SimpleAttribute != null)
                                    {
                                        Logger.Debug($"        Operand {j++}(SimpleAttribute): TypeId: '{operand.SimpleAttribute.TypeId}'");
                                        Logger.Debug($"                                        BrowsePath: '{string.Join(", ", operand.SimpleAttribute.BrowsePaths.ToArray())}'");
                                        Logger.Debug($"                                        AttributeId: '{operand.SimpleAttribute.AttributeId}'");
                                        Logger.Debug($"                                        IndexRange: '{operand.SimpleAttribute.IndexRange}'");
                                    }
                                    if (operand.Attribute != null)
                                    {
                                        Logger.Debug($"        Operand {j++}(Attribute): TypeId: '{operand.Attribute.NodeId}'");
                                        Logger.Debug($"                                  Alias: '{operand.Attribute.Alias}'");
                                        Logger.Debug($"                                  BrowsePath: '{operand.Attribute.BrowsePath}'");
                                        Logger.Debug($"                                  AttributeId: '{operand.Attribute.AttributeId}'");
                                        Logger.Debug($"                                  IndexRange: '{operand.Attribute.IndexRange}'");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// Returns a list of all published nodes for a specific endpoint in config file format.
        /// </summary>
        /// <returns></returns>
        public List<PublisherConfigurationFileEntryModel> GetPublisherConfigurationFileEntries(Guid endpointId, bool getAll, out uint nodeConfigVersion)
        {
            List<PublisherConfigurationFileEntryModel> publisherConfigurationFileEntries = new List<PublisherConfigurationFileEntryModel>();
            nodeConfigVersion = (uint)NodeConfigVersion;
            try
            {
                PublisherNodeConfigurationSemaphore.Wait();

                try
                {
                    OpcSessionsListSemaphore.Wait();

                    // itereate through all sessions, subscriptions and monitored items and create config file entries
                    foreach (var session in OpcSessions)
                    {
                        bool sessionLocked = false;
                        try
                        {
                            sessionLocked = session.LockSessionAsync().Result;
                            if (sessionLocked && (endpointId.Equals(Guid.Empty) || session.EndpointId.Equals(endpointId)))
                            {
                                PublisherConfigurationFileEntryModel publisherConfigurationFileEntry = new PublisherConfigurationFileEntryModel();

                                publisherConfigurationFileEntry.EndpointId = session.EndpointId;
                                publisherConfigurationFileEntry.EndpointName = session.EndpointName;
                                publisherConfigurationFileEntry.EndpointUrl = new Uri(session.EndpointUrl);
                                publisherConfigurationFileEntry.OpcAuthenticationMode = session.OpcAuthenticationMode;
                                publisherConfigurationFileEntry.EncryptedAuthCredential = session.EncryptedAuthCredential;
                                publisherConfigurationFileEntry.UseSecurity = session.UseSecurity;

                                foreach (var subscription in session.OpcSubscriptions)
                                {
                                    if (publisherConfigurationFileEntry.OpcNodes == null)
                                    {
                                        publisherConfigurationFileEntry.OpcNodes = new List<OpcNodeOnEndpointModel>();
                                    }
                                    foreach (var monitoredItem in subscription.OpcMonitoredItems)
                                    {
                                        // ignore items tagged to stop
                                        if (monitoredItem.State != OpcMonitoredItemState.RemovalRequested || getAll == true)
                                        {
                                            OpcNodeOnEndpointModel opcNodeOnEndpoint = new OpcNodeOnEndpointModel(monitoredItem.OriginalId)
                                            {
                                                OpcPublishingInterval = subscription.RequestedPublishingIntervalFromConfiguration ? subscription.RequestedPublishingInterval : (int?)null,
                                                OpcSamplingInterval = monitoredItem.RequestedSamplingIntervalFromConfiguration ? monitoredItem.RequestedSamplingInterval : (int?)null,
                                                Key = monitoredItem.Key,
                                                DisplayName = monitoredItem.DisplayNameFromConfiguration ? monitoredItem.DisplayName : null,
                                                HeartbeatInterval = monitoredItem.HeartbeatIntervalFromConfiguration ? (int?)monitoredItem.HeartbeatInterval : null,
                                                SkipFirst = monitoredItem.SkipFirstFromConfiguration ? (bool?)monitoredItem.SkipFirst : null,
                                                IotCentralItemPublishMode = monitoredItem.IotCentralItemPublishMode
                                            };

                                            publisherConfigurationFileEntry.OpcNodes.Add(opcNodeOnEndpoint);
                                        }
                                    }
                                }
                                foreach (var subscription in session.OpcEventSubscriptions)
                                {
                                    if (publisherConfigurationFileEntry.OpcEvents == null)
                                    {
                                        publisherConfigurationFileEntry.OpcEvents = new List<OpcEventOnEndpointModel>();
                                    }
                                    foreach (var monitoredItem in subscription.OpcMonitoredItems)
                                    {
                                        // ignore items tagged to stop
                                        if (monitoredItem.State != OpcMonitoredItemState.RemovalRequested || getAll == true)
                                        {
                                            OpcEventOnEndpointModel opcEventOnEndpointModel = new OpcEventOnEndpointModel(monitoredItem.EventConfiguration);
                                            publisherConfigurationFileEntry.OpcEvents.Add(opcEventOnEndpointModel);
                                        }
                                    }
                                }
                                publisherConfigurationFileEntries.Add(publisherConfigurationFileEntry);
                            }
                        }
                        finally
                        {
                            if (sessionLocked)
                            {
                                session.ReleaseSession();
                            }
                        }
                    }
                    nodeConfigVersion = (uint)NodeConfigVersion;
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, "Reading configuration file entries failed.");
                publisherConfigurationFileEntries = null;
            }
            finally
            {
                PublisherNodeConfigurationSemaphore.Release();
            }
            return publisherConfigurationFileEntries;
        }

        /// <summary>
        /// Returns a list of all configured nodes in NodeId format.
        /// </summary>
        /// <returns></returns>
        public async Task<List<PublisherConfigurationFileEntryLegacyModel>> GetPublisherConfigurationFileEntriesAsNodeIdsAsync(string endpointUrl)
        {
            List<PublisherConfigurationFileEntryLegacyModel> publisherConfigurationFileEntriesLegacy = new List<PublisherConfigurationFileEntryLegacyModel>();
            try
            {
                await PublisherNodeConfigurationSemaphore.WaitAsync().ConfigureAwait(false);

                try
                {
                    await OpcSessionsListSemaphore.WaitAsync().ConfigureAwait(false);

                    // itereate through all sessions, subscriptions and monitored items and create config file entries
                    foreach (var session in OpcSessions)
                    {
                        bool sessionLocked = false;
                        try
                        {
                            sessionLocked = await session.LockSessionAsync().ConfigureAwait(false);
                            if (sessionLocked && (endpointUrl == null || session.EndpointUrl.Equals(endpointUrl, StringComparison.OrdinalIgnoreCase)))
                            {
                                foreach (var subscription in session.OpcSubscriptions)
                                {
                                    foreach (var monitoredItem in subscription.OpcMonitoredItems)
                                    {
                                        // ignore items tagged to stop
                                        if (monitoredItem.State != OpcMonitoredItemState.RemovalRequested)
                                        {
                                            PublisherConfigurationFileEntryLegacyModel publisherConfigurationFileEntryLegacy = new PublisherConfigurationFileEntryLegacyModel();
                                            publisherConfigurationFileEntryLegacy.EndpointUrl = new Uri(session.EndpointUrl);
                                            publisherConfigurationFileEntryLegacy.NodeId = null;
                                            publisherConfigurationFileEntryLegacy.OpcNodes = null;

                                            if (monitoredItem.ConfigType == OpcMonitoredItemConfigurationType.ExpandedNodeId)
                                            {
                                                // for certain scenarios we support returning the NodeId format even so the
                                                // actual configuration of the node was in ExpandedNodeId format
                                                publisherConfigurationFileEntryLegacy.EndpointUrl = new Uri(session.EndpointUrl);
                                                publisherConfigurationFileEntryLegacy.NodeId = new NodeId(monitoredItem.ConfigExpandedNodeId.Identifier, (ushort)session.GetNamespaceIndexUnlocked(monitoredItem.ConfigExpandedNodeId?.NamespaceUri));
                                                publisherConfigurationFileEntriesLegacy.Add(publisherConfigurationFileEntryLegacy);
                                            }
                                            else
                                            {
                                                // we do not convert nodes with legacy configuration to the new format to keep backward
                                                // compatibility with external configurations.
                                                // the conversion would only be possible, if the session is connected, to have access to the
                                                // server namespace array.
                                                publisherConfigurationFileEntryLegacy.EndpointUrl = new Uri(session.EndpointUrl);
                                                publisherConfigurationFileEntryLegacy.NodeId = monitoredItem.ConfigNodeId;
                                                publisherConfigurationFileEntriesLegacy.Add(publisherConfigurationFileEntryLegacy);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        finally
                        {
                            if (sessionLocked)
                            {
                                session.ReleaseSession();
                            }
                        }
                    }
                }
                finally
                {
                    OpcSessionsListSemaphore.Release();
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, "Creation of configuration file entries failed.");
                publisherConfigurationFileEntriesLegacy = null;
            }
            finally
            {
                PublisherNodeConfigurationSemaphore.Release();
            }
            return publisherConfigurationFileEntriesLegacy;
        }

        /// <summary>
        /// Updates the configuration file to persist all currently published nodes
        /// </summary>
        public async Task UpdateNodeConfigurationFileAsync()
        {
            try
            {
                // itereate through all sessions, subscriptions and monitored items and create config file entries
                List<PublisherConfigurationFileEntryModel> publisherNodeConfiguration = GetPublisherConfigurationFileEntries(Guid.Empty, true, out uint nodeConfigVersion);

                // update the config file
                try
                {
                    await PublisherNodeConfigurationFileSemaphore.WaitAsync().ConfigureAwait(false);
                    await File.WriteAllTextAsync(PublisherNodeConfigurationFilename, JsonConvert.SerializeObject(publisherNodeConfiguration, Formatting.Indented)).ConfigureAwait(false);
                }
                finally
                {
                    PublisherNodeConfigurationFileSemaphore.Release();
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, "Update of node configuration file failed.");
            }
        }

        private List<NodePublishingConfigurationModel> _nodePublishingConfiguration;
        private List<PublisherConfigurationFileEntryLegacyModel> _configurationFileEntries;
        private List<EventConfigurationModel> _eventConfiguration;

        private static readonly object _singletonLock = new object();
        private static IPublisherNodeConfiguration _instance = null;
    }
}