using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using OpcPublisher;

namespace Microsoft.Azure.IIoT.Modules.OpcUa.Publisher.AIT
{
    /// <summary>
    /// Model class for a get configured event nodes on endpoint response.
    /// </summary>
    public class GetConfiguredEventNodesOnEndpointMethodResponseModel
    {
        public GetConfiguredEventNodesOnEndpointMethodResponseModel()
        {
            EventNodes = new List<OpcEventOnEndpointModel>();
        }

        /// <param name="nodes"></param>
        public GetConfiguredEventNodesOnEndpointMethodResponseModel(List<OpcEventOnEndpointModel> nodes)
        {
            EventNodes = nodes;
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public string EndpointUrl { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public List<OpcEventOnEndpointModel> EventNodes { get; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public ulong? ContinuationToken { get; set; }
    }
}
