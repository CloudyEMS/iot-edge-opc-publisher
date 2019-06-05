using Microsoft.Azure.Devices.Client;
using Moq;
using Newtonsoft.Json;
using opcpublisher.AIT;
using OpcPublisher;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Microsoft.Azure.IIoT.Modules.OpcUa.Publisher.Tests.AIT
{
    using static Program;

    [Collection("Need PLC and publisher config")]
    public sealed class CommandUnitTests : IDisposable
    {
        public CommandUnitTests(ITestOutputHelper output, PlcOpcUaServerFixture server)
        {
            // xunit output
            _output = output;
            _server = server;

            // init configuration objects
            TelemetryConfiguration = PublisherTelemetryConfiguration.Instance;
            Diag = PublisherDiagnostics.Instance;
        }

        /// <summary>
        /// Implement IDisposable.
        /// </summary>
        void Dispose(bool disposing)
        {
            if (disposing)
            {
                // dispose managed resources
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
        /// Test reading different configuration files and creating the correct internal data structures for command handling.
        /// </summary>
        [Theory]
        [Trait("Configuration", "File")]
        [Trait("ConfigurationSetting", "SimpleCommandConfiguration")]
        [MemberData(nameof(PnPlcCommandSimple))]
        public async void CreateOpcCommandMonitoring(string testFilename, int configuredSessions,
            int configuredSubscriptions, int configuredMonitoredItems, int configuredMonitoredEvents)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/ait/{testFilename}";
            fqTempFilename = $"{Directory.GetCurrentDirectory()}/tempdata/{methodName}_{testFilename}";
            if (File.Exists(fqTempFilename))
            {
                File.Delete(fqTempFilename);
            }
            File.Copy(fqTestFilename, fqTempFilename);
            PublisherNodeConfiguration.PublisherNodeConfigurationFilename = fqTempFilename;
            _output.WriteLine($"now testing: {PublisherNodeConfiguration.PublisherNodeConfigurationFilename}");
            Assert.True(File.Exists(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));

            UnitTestHelper.SetPublisherDefaults();

            try
            {
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == configuredSessions, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == configuredSessions, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == configuredSubscriptions, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured == configuredMonitoredEvents, "wrong # of events");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == configuredMonitoredItems, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, " +
                                  $"toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}, events configured {NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured}, " +
                                  $"monitored {NodeConfiguration.NumberOfOpcEventMonitoredItemsMonitored}");

                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));

                Assert.True(_configurationFileEntries[0].OpcNodes.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcSamplingInterval == 2000);
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcPublishingInterval == 5000);
                Assert.True(_configurationFileEntries[0].OpcNodes[0].IotCentralItemPublishMode == IotCentralItemPublishMode.Command);

                // mock IoTHub communication
                var hubMockBase = new Mock<HubCommunicationBase>();
                var hubMock = hubMockBase.As<IHubCommunication>();
                hubMock.CallBase = true;
                Hub = hubMock.Object;

                // configure hub client mock
                var hubClientMockBase = new Mock<HubClient>();
                var hubClientMock = hubClientMockBase.As<IHubClient>();

                IotHubCommunication.IotHubClient = hubClientMock.Object;
                Hub.InitHubCommunicationAsync(hubClientMockBase.Object, true, true).Wait();
                var methodRequest = new MethodRequest("GetMonitoredItems", Encoding.ASCII.GetBytes("{\"commandParameters\":{ \"SubscriptionId\" : 41 } }"));
                hubClientMock.Setup(x => x.DefaultCommandHandlerAsync(It.IsAny<MethodRequest>(), null)).Returns(Task.FromResult(new MethodResponse(200)));
                var methodResponse = await IotHubCommunication.IotHubClient.DefaultCommandHandlerAsync(methodRequest, null);

                Assert.True(methodResponse != null);
                Assert.True(methodResponse.Status == 200);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
            }
        }

        public static IEnumerable<object[]> PnPlcCommandSimple =>
           new List<object[]>
           {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_command_simple.json"),
                    // # of configured sessions
                    1,
                    // # of configured subscriptions
                    1,
                    // # of configured monitored items
                    1,
                    // # of configured event items
                    0
                }
           };

        /// <summary>
        /// Telemetry configuration object.
        /// </summary>
        public static IPublisherTelemetryConfiguration TelemetryConfiguration { get; set; }

        /// <summary>
        /// Diagnostics object.
        /// </summary>
        public static IPublisherDiagnostics Diag { get; set; }

        /// <summary>
        /// Node configuration object.
        /// </summary>
        public static IPublisherNodeConfiguration NodeConfiguration { get; set; }

        private readonly ITestOutputHelper _output;
        private readonly PlcOpcUaServerFixture _server;
        private static List<PublisherConfigurationFileEntryLegacyModel> _configurationFileEntries;
    }
}
