namespace OpcPublisher
{
    using Microsoft.Azure.Devices.Client;
    using Moq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;
    using static Program;

    /// <summary>
    /// Make sure you have the registry running with legacy access on port 2375 without tls
    /// </summary>
    [Collection("Need PLC and publisher config")]
    public sealed class TelemetryUnitTests
    {
        public TelemetryUnitTests(ITestOutputHelper output)
        {
            _output = output;
            TelemetryConfiguration = PublisherTelemetryConfiguration.Instance;
            Diag = PublisherDiagnostics.Instance;
        }

        /// <summary>
        /// Test telemetry is sent to the hub.
        /// </summary>
        [Theory]
        [Trait("Telemetry", "All")]
        [Trait("TelemetryFunction", "Basic")]
        [Trait("Category", "Integration")]
        [MemberData(nameof(PnPlcCurrentTime))]
        public async Task TelemetryIsSentAsync(string testFilename, int configuredSessions,
            int configuredSubscriptions, int configuredMonitoredItems)
        {
            _output.WriteLine($"now testing: {testFilename}");

            // mock IoTHub communication
            var hubMockBase = new Mock<HubCommunicationBase>();
            var hubMock = hubMockBase.As<IHubCommunication>();
            hubMock.CallBase = true;
            Hub = hubMock.Object;

            // configure hub client mock
            var hubClientMockBase = new Mock<HubClient>();
            var hubClientMock = hubClientMockBase.As<IHubClient>();
            int eventsReceived = 0;
            hubClientMock.Setup(m => m.SendEventAsync(It.IsAny<Message>())).Callback<Message>(m => eventsReceived++).Returns(Task.CompletedTask);
            IotHubCommunication.IotHubClient = hubClientMock.Object;
            Hub.InitHubCommunicationAsync(hubClientMockBase.Object, true, true).Wait();

            UnitTestHelper.SetPublisherDefaults();

            using (new ExecutionContext(testFilename))
            {
                AssertPreconditions(configuredSessions, configuredSubscriptions, configuredMonitoredItems);

                long messagesAtStart = HubCommunicationBase.SentMessages;
                int seconds = UnitTestHelper.WaitTilItemsAreMonitoredAndFirstEventReceived();
                long messagesAfterConnect = HubCommunicationBase.SentMessages;
                await Task.Delay(2500).ConfigureAwait(false);
                long messagesAfterDelay = HubCommunicationBase.SentMessages;
                _output.WriteLine($"# of messages at start: {messagesAtStart}, # messages after connect: {messagesAfterConnect}, # messages after delay: {messagesAfterDelay}");
                _output.WriteLine($"waited {seconds} seconds till monitoring started, events generated {eventsReceived}");

                hubClientMock.VerifySet(m => m.ProductInfo = "OpcPublisher");
                Assert.True(messagesAfterDelay - messagesAfterConnect == 2);
            }
        }

        /// <summary>
        /// Test telemetry is sent to the hub using node with static value.
        /// </summary>
        [Theory]
        [Trait("Telemetry", "All")]
        [Trait("TelemetryFunction", "Basic")]
        [Trait("Category", "Integration")]
        [MemberData(nameof(PnPlcProductName))]
        public async Task TelemetryIsSentWithStaticNodeValueAsync(string testFilename, int configuredSessions,
            int configuredSubscriptions, int configuredMonitoredItems)
        {
            _output.WriteLine($"now testing: {testFilename}");

            // mock IoTHub communication
            var hubMockBase = new Mock<HubCommunicationBase>();
            var hubMock = hubMockBase.As<IHubCommunication>();
            hubMock.CallBase = true;
            Hub = hubMock.Object;

            // configure hub client mock
            var hubClientMockBase = new Mock<HubClient>();
            var hubClientMock = hubClientMockBase.As<IHubClient>();
            int eventsReceived = 0;
            hubClientMock.Setup(m => m.SendEventAsync(It.IsAny<Message>())).Callback<Message>(m => eventsReceived++).Returns(Task.CompletedTask);
            IotHubCommunication.IotHubClient = hubClientMock.Object;
            Hub.InitHubCommunicationAsync(hubClientMockBase.Object, true, true).Wait();

            UnitTestHelper.SetPublisherDefaults();

            using (new ExecutionContext(testFilename))
            {
                AssertPreconditions(configuredSessions, configuredSubscriptions, configuredMonitoredItems);

                long eventsAtStart = HubCommunicationBase.NumberOfDataChangeEvents;
                int seconds = UnitTestHelper.WaitTilItemsAreMonitored();
                long eventsAfterConnect = HubCommunicationBase.NumberOfDataChangeEvents;
                await Task.Delay(3000).ConfigureAwait(false);
                long eventsAfterDelay = HubCommunicationBase.NumberOfDataChangeEvents;
                _output.WriteLine($"# of events at start: {eventsAtStart}, # events after connect: {eventsAfterConnect}, # events after delay: {eventsAfterDelay}");
                _output.WriteLine($"waited {seconds} seconds till monitoring started, events generated {eventsReceived}");

                hubClientMock.VerifySet(m => m.ProductInfo = "OpcPublisher");
                Assert.Equal(1, eventsAfterDelay - eventsAtStart);
            }
        }

        /// <summary>
        /// Test first event is skipped.
        /// </summary>
        [Theory]
        [Trait("Telemetry", "All")]
        [Trait("TelemetryFunction", "SkipFirst")]
        [Trait("Category", "Integration")]
        [MemberData(nameof(PnPlcCurrentTime))]
        public async Task FirstTelemetryEventIsSkippedAsync(string testFilename, int configuredSessions,
            int configuredSubscriptions, int configuredMonitoredItems)
        {
            _output.WriteLine($"now testing: {testFilename}");

            UnitTestHelper.SetPublisherDefaults();
            OpcMonitoredItem.SkipFirstDefault = true;

            // mock IoTHub communication
            var hubMockBase = new Mock<HubCommunicationBase>();
            var hubMock = hubMockBase.As<IHubCommunication>();
            hubMock.CallBase = true;
            Hub = hubMock.Object;

            // configure hub client mock
            var hubClientMockBase = new Mock<HubClient>();
            var hubClientMock = hubClientMockBase.As<IHubClient>();
            int eventsReceived = 0;
            hubClientMock.Setup(m => m.SendEventAsync(It.IsAny<Message>())).Callback<Message>(m => eventsReceived++).Returns(Task.CompletedTask);
            IotHubCommunication.IotHubClient = hubClientMock.Object;
            Hub.InitHubCommunicationAsync(hubClientMockBase.Object, true, true).Wait();

            using (new ExecutionContext(testFilename))
            {
                AssertPreconditions(configuredSessions, configuredSubscriptions, configuredMonitoredItems);

                long eventsAtStart = HubCommunicationBase.NumberOfDataChangeEvents;
                int seconds = UnitTestHelper.WaitTilItemsAreMonitored();
                long eventsAfterConnect = HubCommunicationBase.NumberOfDataChangeEvents;
                await Task.Delay(1900).ConfigureAwait(false);
                long eventsAfterDelay = HubCommunicationBase.NumberOfDataChangeEvents;
                _output.WriteLine($"# of events at start: {eventsAtStart}, # events after connect: {eventsAfterConnect}, # events after delay: {eventsAfterDelay}");
                _output.WriteLine($"waited {seconds} seconds till monitoring started, events generated {eventsReceived}");

                hubClientMock.VerifySet(m => m.ProductInfo = "OpcPublisher");
                Assert.Equal(1, eventsAfterDelay - eventsAtStart);
            }
        }

        /// <summary>
        /// Test first event is skipped using a node with static value.
        /// </summary>
        [Theory]
        [Trait("Telemetry", "All")]
        [Trait("TelemetryFunction", "SkipFirst")]
        [Trait("Category", "Integration")]
        [MemberData(nameof(PnPlcProductName))]
        public async Task FirstTelemetryEventIsSkippedWithStaticNodeValueAsync(string testFilename, int configuredSessions,
            int configuredSubscriptions, int configuredMonitoredItems)
        {
            _output.WriteLine($"now testing: {testFilename}");

            UnitTestHelper.SetPublisherDefaults();
            OpcMonitoredItem.HeartbeatIntervalDefault = 0;
            OpcMonitoredItem.SkipFirstDefault = true;

            // mock IoTHub communication
            var hubMockBase = new Mock<HubCommunicationBase>();
            var hubMock = hubMockBase.As<IHubCommunication>();
            hubMock.CallBase = true;
            Hub = hubMock.Object;

            // configure hub client mock
            var hubClientMockBase = new Mock<HubClient>();
            var hubClientMock = hubClientMockBase.As<IHubClient>();
            int eventsReceived = 0;
            hubClientMock.Setup(m => m.SendEventAsync(It.IsAny<Message>())).Callback<Message>(m => eventsReceived++).Returns(Task.CompletedTask);
            IotHubCommunication.IotHubClient = hubClientMock.Object;
            Hub.InitHubCommunicationAsync(hubClientMockBase.Object, true, true).Wait();

            using(new ExecutionContext(testFilename))
            {
                AssertPreconditions(configuredSessions, configuredSubscriptions, configuredMonitoredItems);

                long eventsAtStart = HubCommunicationBase.NumberOfDataChangeEvents;
                int seconds = UnitTestHelper.WaitTilItemsAreMonitored();
                long eventsAfterConnect = HubCommunicationBase.NumberOfDataChangeEvents;
                await Task.Delay(3000).ConfigureAwait(false);
                long eventsAfterDelay = HubCommunicationBase.NumberOfDataChangeEvents;
                _output.WriteLine($"# of events at start: {eventsAtStart}, # events after connect: {eventsAfterConnect}, # events after delay: {eventsAfterDelay}");
               _output.WriteLine($"waited {seconds} seconds till monitoring started, events generated {eventsReceived}");

                hubClientMock.VerifySet(m => m.ProductInfo = "OpcPublisher");
                Assert.Equal(0, eventsAfterDelay - eventsAtStart);
            }
        }

        /// <summary>
        /// Test heartbeat is working on a node with static value.
        /// </summary>
        [Theory]
        [Trait("Telemetry", "All")]
        [Trait("TelemetryFunction", "Heartbeat")]
        [Trait("Category", "Integration")]
        [MemberData(nameof(PnPlcProductNameHeartbeat2))]
        public async Task HeartbeatOnStaticNodeValueIsWorkingAsync(string testFilename, int configuredSessions,
            int configuredSubscriptions, int configuredMonitoredItems)
        {
            _output.WriteLine($"now testing: {testFilename}");

            OpcMonitoredItem.HeartbeatIntervalDefault = 0;

            // mock IoTHub communication
            var hubMockBase = new Mock<HubCommunicationBase>();
            var hubMock = hubMockBase.As<IHubCommunication>();
            hubMock.CallBase = true;
            SendHub = hubMock.Object;

            // configure hub client mock
            var hubClientMockBase = new Mock<HubClient>();
            var hubClientMock = hubClientMockBase.As<IHubClient>();
            int eventsReceived = 0;
            hubClientMock.Setup(m => m.SendEventAsync(It.IsAny<Message>())).Callback<Message>(m => eventsReceived++).Returns(Task.CompletedTask);
            IotHubCommunication.IotHubClient = hubClientMock.Object;
            SendHub.InitHubCommunicationAsync(hubClientMockBase.Object, true, true).Wait();

            using (new ExecutionContext(testFilename))
            {
                AssertPreconditions(configuredSessions, configuredSubscriptions, configuredMonitoredItems);

                long eventsAtStart = HubCommunicationBase.NumberOfDataChangeEvents;
                int seconds = UnitTestHelper.WaitTilItemsAreMonitored();
                long eventsAfterConnect = HubCommunicationBase.NumberOfDataChangeEvents;
                await Task.Delay(3000).ConfigureAwait(false);
                long eventsAfterDelay = HubCommunicationBase.NumberOfDataChangeEvents;
                _output.WriteLine($"# of events at start: {eventsAtStart}, # events after connect: {eventsAfterConnect}, # events after delay: {eventsAfterDelay}");
                _output.WriteLine($"waited {seconds} seconds till monitoring started, events generated {eventsReceived}");

                // hubClientMock.VerifySet(m => m.ProductInfo = "OpcPublisher");
                Assert.Equal(2, eventsAfterDelay - eventsAtStart);
            }
        }

        /// <summary>
        /// Test heartbeat is working on a node with static value with skip first true.
        /// </summary>
        [Theory]
        [Trait("Telemetry", "All")]
        [Trait("TelemetryFunction", "Heartbeat")]
        [Trait("Category", "Integration")]
        [MemberData(nameof(PnPlcProductNameHeartbeat2SkipFirst))]
        public async Task HeartbeatWithSkipFirstOnStaticNodeValueIsWorkingAsync(string testFilename, int configuredSessions,
            int configuredSubscriptions, int configuredMonitoredItems)
        {
            _output.WriteLine($"now testing: {testFilename}");

            OpcMonitoredItem.HeartbeatIntervalDefault = 0;

            // mock IoTHub communication
            var hubMockBase = new Mock<HubCommunicationBase>();
            var hubMock = hubMockBase.As<IHubCommunication>();
            hubMock.CallBase = true;
            SendHub = hubMock.Object;

            // configure hub client mock
            var hubClientMockBase = new Mock<HubClient>();
            var hubClientMock = hubClientMockBase.As<IHubClient>();
            int eventsReceived = 0;
            hubClientMock.Setup(m => m.SendEventAsync(It.IsAny<Message>())).Callback<Message>(m => eventsReceived++).Returns(Task.CompletedTask);
            IotHubCommunication.IotHubClient = hubClientMock.Object;
            SendHub.InitHubCommunicationAsync(hubClientMockBase.Object, true, true).Wait();

            using (new ExecutionContext(testFilename))
            {
                AssertPreconditions(configuredSessions, configuredSubscriptions, configuredMonitoredItems);

                long eventsAtStart = HubCommunicationBase.NumberOfDataChangeEvents;
                int seconds = UnitTestHelper.WaitTilItemsAreMonitoredAndFirstEventReceived();
                long eventsAfterConnect = HubCommunicationBase.NumberOfDataChangeEvents;
                await Task.Delay(3000).ConfigureAwait(false);
                long eventsAfterDelay = HubCommunicationBase.NumberOfDataChangeEvents;
                _output.WriteLine($"# of events at start: {eventsAtStart}, # events after connect: {eventsAfterConnect}, # events after delay: {eventsAfterDelay}");
                _output.WriteLine($"waited {seconds} seconds till monitoring started, events generated {eventsReceived}");

                hubClientMock.VerifySet(m => m.ProductInfo = "OpcPublisher");
                Assert.Equal(2, eventsAfterDelay - eventsAtStart);
            }
        }

        // published nodes configuration file
        // # of configured sessions
        // # of configured subscriptions
        // # of configured monitored items
        public static IEnumerable<object[]> PnPlcCurrentTime =>
            new List<object[]>
            {
                new object[]
                {
                    "pn_plc_currenttime.json", 1, 1, 1
                },
            };

        public static IEnumerable<object[]> PnPlcProductName =>
            new List<object[]>
            {
                new object[]
                {
                    "pn_plc_productname.json", 1, 1, 1
                },
            };

        public static IEnumerable<object[]> PnPlcProductNameHeartbeat2 =>
            new List<object[]>
            {
                new object[]
                {
                    "pn_plc_productname_heartbeatinterval_2.json", 1, 1, 1
                },
            };

        public static IEnumerable<object[]> PnPlcProductNameHeartbeat2SkipFirst =>
            new List<object[]>
            {
                new object[]
                {
                    $"pn_plc_productname_heartbeatinterval_2_skipfirst.json", 1, 1, 1
                },
            };

        private readonly ITestOutputHelper _output;

        private void AssertPreconditions(int configuredSessions, int configuredSubscriptions, int configuredMonitoredItems)
        {
            _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
            _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
            _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");

            Assert.True(NodeConfiguration.OpcSessions.Count == configuredSessions, "wrong # of sessions");
            Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == configuredSessions, "wrong # of sessions");
            Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == configuredSubscriptions, "wrong # of subscriptions");
            Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == configuredMonitoredItems, "wrong # of monitored items");
        }

        private class ExecutionContext : IDisposable
        {
            public ExecutionContext(string testFilename)
            {
                var methodName = UnitTestHelper.GetMethodName();
                var fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/telemetry/{testFilename}";
                var fqTempFilename = $"{Directory.GetCurrentDirectory()}/tempdata/{methodName}_{testFilename}";
                if (File.Exists(fqTempFilename))
                {
                    File.Delete(fqTempFilename);
                }
                File.Copy(fqTestFilename, fqTempFilename);
                PublisherNodeConfiguration.PublisherNodeConfigurationFilename = fqTempFilename;
                Assert.True(File.Exists(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));

                NodeConfiguration = new PublisherNodeConfiguration(); // PublisherNodeConfiguration.Instance;
            }

            public void Dispose()
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                IotHubCommunication.IotHubClient = null;
                Hub?.Dispose();
                Hub = null;
            }
        }
    }
}