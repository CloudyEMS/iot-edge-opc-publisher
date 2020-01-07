using FluentAssertions;
using Microsoft.Azure.Devices.Client;
using Moq;
using OpcPublisher;
using Serilog;
using System;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace OpcPublisher.Tests.AIT
{
    public class HubCommunicationBaseTests
    {
        [Fact]
        public async Task MonitoredItemsProcessorAsync_ShouldWaitForTimeout()
        {
            int timeoutSeconds = 10;
            double timeoutAcceptanceThresholdSeconds = 0.1;
            AutoResetEvent messageReceivedEvent = new AutoResetEvent(false);

            var itemProcessor = new HubCommunicationBase();

            Program.Logger = new LoggerConfiguration().CreateLogger();

            itemProcessor.SendIntervalSeconds = timeoutSeconds;
            itemProcessor.HubMessageSize = 100000;

            Program.TelemetryConfiguration = new PublisherTelemetryConfiguration();

            DateTime lastMessageSent = DateTime.UtcNow;
            TimeSpan timeBetweenMessages = new TimeSpan();
            long streamLength = 0;
            int messagesReceived = 0;

            var hubClient = new Mock<IHubClient>();
            hubClient
                .Setup(client => client.SendEventAsync(It.IsAny<Message>()))
                .Returns(Task.CompletedTask)
                .Callback((Message hubMessage) =>
                {
                    timeBetweenMessages = DateTime.UtcNow.Subtract(lastMessageSent);
                    lastMessageSent = DateTime.UtcNow;
                    streamLength = hubMessage.BodyStream.Length;
                    messageReceivedEvent.Set();
                    messagesReceived++;

                });

            MessageData message = new MessageData
            {
                DataChangeMessageData = new DataChangeMessageData
                {
                    DisplayName = "Test",
                    EndpointUrl = "abc123",
                    EndpointId = "qwe"
                }
            };

            int messageLength = 50;

            _ = await Task.Factory.StartNew(() => itemProcessor.InitHubCommunicationAsync(hubClient.Object, false, true), TaskCreationOptions.LongRunning);

            itemProcessor.Enqueue(message);
            itemProcessor.Enqueue(message);
            messageReceivedEvent.WaitOne((timeoutSeconds + 5) * 1000).Should().BeTrue();

            itemProcessor.Enqueue(message);
            itemProcessor.Enqueue(message);
            messageReceivedEvent.WaitOne((timeoutSeconds + 5) * 1000).Should().BeTrue();

            timeBetweenMessages.TotalMilliseconds.Should().BeApproximately(timeoutSeconds * 1000, timeoutAcceptanceThresholdSeconds * 1000);

            //2 messages are sent
            //for every message, except the last one, there is one "," added
            //and the received messages begins with '[' and ends with ']'W
            streamLength.Should().Be((messageLength + 1) * 2 + 1);
        }

        [Fact]
        public async Task MonitoredItemsProcessorAsync_ShouldWaitFilledBuffer()
        {
            uint buffersize = 1000;
            ManualResetEvent messageReceivedEvent = new ManualResetEvent(false);

            var itemProcessor = new HubCommunicationBase();
            Program.Logger = new LoggerConfiguration().CreateLogger();
            Program.TelemetryConfiguration = new PublisherTelemetryConfiguration();
            itemProcessor.SendIntervalSeconds = 0;
            itemProcessor.HubMessageSize = buffersize;

            long streamLength = 0;

            var hubClient = new Mock<IHubClient>();
            hubClient
                .Setup(client => client.SendEventAsync(It.IsAny<Message>()))
                .Returns(Task.CompletedTask)
                .Callback((Message hubMessage) =>
                {
                    streamLength = hubMessage.BodyStream.Length;
                    messageReceivedEvent.Set();
                });

            MessageData message = new MessageData
            {
                DataChangeMessageData = new DataChangeMessageData
                {
                    DisplayName = "Test",
                    EndpointUrl = "abc123",
                    EndpointId = "qwe"
                }
            };

            int messageLength = 50;

            // the system properties are MessageId (max 128 byte), Sequence number (ulong), ExpiryTime (DateTime) and more. ideally we get that from the client.
            Message tempMsg = new Message();
            int systemPropertyLength = 128 + sizeof(ulong) + tempMsg.ExpiryTimeUtc.ToString(CultureInfo.InvariantCulture).Length;
            int applicationPropertyLength = Encoding.UTF8.GetByteCount($"iothub-content-type=application/opcua+uajson") + Encoding.UTF8.GetByteCount($"iothub-content-encoding=UTF-8");
            int jsonSquareBracketLength = 2;

            int bufferSizeWithoutOverhead = (int)buffersize - systemPropertyLength - applicationPropertyLength - jsonSquareBracketLength;

            //need to reduce buffersize by one for the '[' at the beginning of the message
            //need to add 1 to the message length for the ',' that is added after every enqueued Item
            //messagesToSend should be 1 higher then the messages received because the last one sent is needed to trigger the buffer overflow
            int messagesToSend = (int)((bufferSizeWithoutOverhead - 1) / (messageLength + 1)) + 1;

            _ = await Task.Run(() => itemProcessor.InitHubCommunicationAsync(hubClient.Object, false, true));

            for (int messagesSent = 0; messagesSent < messagesToSend; messagesSent++)
            {
                //Check if no message was received before sending the last item.
                messageReceivedEvent.WaitOne(0).Should().BeFalse();
                itemProcessor.Enqueue(message);
            }

            //Wait a maximum of 2s for the message to arrive.
            messageReceivedEvent.WaitOne(2000).Should().BeTrue();

            //for every message, except the last one, there is one "," added
            //and the received messages begins with '[' and ends with ']'
            streamLength.Should().Be((messageLength + 1) * (messagesToSend - 1) + 1);

        }
    }
}
