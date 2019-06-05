using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using opcpublisher.AIT;


namespace OpcPublisher
{
    using Opc.Ua;
    using System.Diagnostics;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using static OpcApplicationConfiguration;
    using static OpcPublisher.OpcMonitoredItem;
    using static Program;
    public partial class OpcSession
    {
        /// <summary>
        /// Adds a event node to be monitored. If there is no subscription with the requested publishing interval,
        /// one is created.
        /// </summary>
        public async Task<HttpStatusCode> AddEventNodeForMonitoringAsync(NodeId nodeId, ExpandedNodeId expandedNodeId,
            int? opcPublishingInterval, int? opcSamplingInterval, string displayName,
            int? heartbeatInterval, bool? skipFirst, CancellationToken ct, IotCentralItemPublishMode? iotCentralItemPublishMode,
            PublishNodesMethodRequestModel publishEventsMethodData)
        {
            string logPrefix = "AddEventNodeForMonitoringAsync:";
            bool sessionLocked = false;
            try
            {
                sessionLocked = await LockSessionAsync().ConfigureAwait(false);
                if (!sessionLocked || ct.IsCancellationRequested)
                {
                    return HttpStatusCode.Gone;
                }

                // check if there is already a subscription with the same publishing interval, which can be used to monitor the node
                int opcPublishingIntervalForNode = opcPublishingInterval ?? OpcPublishingIntervalDefault;
                IOpcSubscription opcEventSubscription = OpcEventSubscriptions.FirstOrDefault(s => s.RequestedPublishingInterval == opcPublishingIntervalForNode);

                // if there was none found, create one
                if (opcEventSubscription == null)
                {
                    if (opcPublishingInterval == null)
                    {
                        Logger.Information($"{logPrefix} No matching subscription with default publishing interval found.");
                        Logger.Information($"Create a new subscription with a default publishing interval.");
                    }
                    else
                    {
                        Logger.Information($"{logPrefix} No matching subscription with publishing interval of {opcPublishingInterval} found.");
                        Logger.Information($"Create a new subscription with a publishing interval of {opcPublishingInterval}.");
                    }
                    opcEventSubscription = new OpcSubscription(opcPublishingInterval);
                    OpcEventSubscriptions.Add(opcEventSubscription);
                }

                // create objects for publish check
                ExpandedNodeId expandedNodeIdCheck = expandedNodeId;
                NodeId nodeIdCheck = nodeId;
                if (State == SessionState.Connected)
                {
                    if (expandedNodeId == null)
                    {
                        string namespaceUri = _namespaceTable.ToArray().ElementAtOrDefault(nodeId.NamespaceIndex);
                        expandedNodeIdCheck = new ExpandedNodeId(nodeId.Identifier, nodeId.NamespaceIndex, namespaceUri, 0);
                    }
                    if (nodeId == null)
                    {
                        nodeIdCheck = new NodeId(expandedNodeId.Identifier, (ushort)(_namespaceTable.GetIndex(expandedNodeId.NamespaceUri)));
                    }
                }

                // if it is already published, we do nothing, else we create a new monitored event item
                if (!IsEventNodePublishedInSessionInternal(nodeIdCheck, expandedNodeIdCheck))
                {
                    OpcMonitoredItem opcMonitoredItem = null;
                    // add a new item to monitor
                    if (expandedNodeId == null)
                    {
                        opcMonitoredItem = new OpcMonitoredItem(new EventConfigurationModel(publishEventsMethodData.EndpointUrl, publishEventsMethodData.UseSecurity,
                            publishEventsMethodData.OpcEvents[0].Id, publishEventsMethodData.OpcEvents[0].DisplayName, publishEventsMethodData.OpcEvents[0].SelectClauses,
                            publishEventsMethodData.OpcEvents[0].WhereClause, publishEventsMethodData.OpcEvents[0].IotCentralEventPublishMode), EndpointUrl);
                    }
                    else
                    {
                        opcMonitoredItem = new OpcMonitoredItem(new EventConfigurationModel(publishEventsMethodData.EndpointUrl, publishEventsMethodData.UseSecurity,
                            expandedNodeId.ToString(), publishEventsMethodData.OpcEvents[0].DisplayName, publishEventsMethodData.OpcEvents[0].SelectClauses,
                            publishEventsMethodData.OpcEvents[0].WhereClause, publishEventsMethodData.OpcEvents[0].IotCentralEventPublishMode), EndpointUrl);
                    }
                    opcEventSubscription.OpcMonitoredItems.Add(opcMonitoredItem);
                    Interlocked.Increment(ref NodeConfigVersion);
                    Logger.Debug($"{logPrefix} Added event item with nodeId '{(expandedNodeId == null ? nodeId.ToString() : expandedNodeId.ToString())}' for monitoring.");

                    // trigger the actual OPC communication with the server to be done
                    ConnectAndMonitorSession.Set();
                    return HttpStatusCode.Accepted;
                }
                else
                {
                    Logger.Debug($"{logPrefix} Node with Id '{(expandedNodeId == null ? nodeId.ToString() : expandedNodeId.ToString())}' is already monitored.");
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, $"{logPrefix} Exception while trying to add node '{(expandedNodeId == null ? nodeId.ToString() : expandedNodeId.ToString())}' for monitoring.");
                return HttpStatusCode.InternalServerError;
            }
            finally
            {
                if (sessionLocked)
                {
                    ReleaseSession();
                }
            }
            return HttpStatusCode.OK;
        }



        /// <summary>
        /// Checks if the event node specified by either the given NodeId or ExpandedNodeId on the given endpoint is published in the session. Caller to take session semaphore.
        /// </summary>
        private bool IsEventNodePublishedInSessionInternal(NodeId nodeId, ExpandedNodeId expandedNodeId)
        {
            try
            {
                foreach (var opcSubscription in OpcEventSubscriptions)
                {
                    if (opcSubscription.OpcMonitoredItems.Any(m => { return m.IsMonitoringThisNode(nodeId, expandedNodeId, _namespaceTable); }))
                    {
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, "Exception");
            }
            return false;
        }
    }
}
