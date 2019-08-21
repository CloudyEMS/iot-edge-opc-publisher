using FluentAssertions;
using Microsoft.Azure.Devices.Client;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OpcPublisher;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.IIoT.Modules.OpcUa.Publisher.Tests.AIT
{
    public class IoTCEventsProcessorTests
    {
        [Fact]
        public async Task MonitoredIoTCEventsProcessorAsync_ShouldSendSingleMessageForEvent()
        {
            Message receivedHubMessage = null;
            using (var cancellationTokenSource = Debugger.IsAttached ? new CancellationTokenSource() : new CancellationTokenSource(2000))
            {
                var hubClient = new Mock<IHubClient>();
                hubClient
                    .Setup(client => client.SendEventAsync(It.IsAny<Message>()))
                    .Returns(Task.CompletedTask)
                    .Callback((Message hubMessage) => {
                        receivedHubMessage = hubMessage;
                        cancellationTokenSource.Cancel();
                    });

                var eventProcessor = new IoTCEventsProcessor(Serilog.Log.Logger, hubClient.Object, true, 0, 512, 0, cancellationTokenSource.Token);

                _ = Task.Run(() => eventProcessor.MonitoredIoTCEventsProcessorAsync());

                MessageData message = new MessageData
                {
                    EventMessageData = new EventMessageData 
                    {
                        DisplayName = "Test"
                    }
                };
                eventProcessor.EnqueueEvent(message);

                while (!cancellationTokenSource.IsCancellationRequested)
                {
                    await Task.Delay(1000);
                }
            }

            receivedHubMessage.BodyStream.Position = 0;
            var receivedMessage = Deserialize<JObject>(receivedHubMessage.BodyStream);
            receivedMessage.Properties().Should().ContainSingle(x => x.Name == "Test");
            receivedMessage.Properties().Should().ContainSingle(x => x.Name == "messageType");
            receivedMessage.Properties().Single(x => x.Name == "messageType").Value.Value<string>().Should().Be("event");
        }


        public static T Deserialize<T>(Stream s)
        {
            using (StreamReader reader = new StreamReader(s))
            using (JsonTextReader jsonReader = new JsonTextReader(reader))
            {
                JsonSerializer ser = new JsonSerializer();
                return ser.Deserialize<T>(jsonReader);
            }
        }
    }
}
