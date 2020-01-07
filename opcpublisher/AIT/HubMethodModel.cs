using System.Collections.Generic;
using Newtonsoft.Json;
using OpcPublisher;

namespace OpcPublisher.AIT
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
        public string EndpointId { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public List<OpcEventOnEndpointModel> EventNodes { get; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public ulong? ContinuationToken { get; set; }
    }

    public class HandleSaveOpcPublishedConfigurationMethodRequestModel
    {
        public HandleSaveOpcPublishedConfigurationMethodRequestModel(string configurationJsonString)
        {
            ConfigurationJsonString = configurationJsonString;
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public string ConfigurationJsonString { get; set; }
    }

    /// <summary>
    /// Model class for a get configured event nodes on endpoint response.
    /// </summary>
    public class GetOpcPublishedConfigurationMethodResponseModel
    {
        public GetOpcPublishedConfigurationMethodResponseModel()
        {
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public string ConfigurationJson { get; set; }
    }

    /// <summary>
    /// Model class for a get configured event nodes on endpoint response.
    /// </summary>
    public class SaveOpcPublishedConfigurationMethodResponseModel
    {
        public SaveOpcPublishedConfigurationMethodResponseModel()
        {
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public bool Success { get; set; }
    }
}
