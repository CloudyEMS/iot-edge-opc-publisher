using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.Devices;
using static OpcPublisher.OpcApplicationConfiguration;
using static OpcPublisher.Program;

namespace OpcPublisher
{
    public partial class IotHubCommunication
    {
        public static IotHubCommunication SendInstance
        {
            get
            {
                lock (_singletonLockExtension)
                {
                    if (_instanceExtension == null)
                    {
                        _instanceExtension = new IotHubCommunication(true);
                    }
                    return _instanceExtension;
                }
            }
        }

        /// <summary>
        /// Ctor for the singleton class.
        /// </summary>
        private IotHubCommunication(bool isIotEdge)
        {
            // check if we got an IoTHub owner connection string
            if (string.IsNullOrEmpty(IotHubOwnerConnectionString))
            {
                Logger.Information("IoT Hub owner connection string not passed as argument.");

                // check if we have an environment variable to register ourselves with IoT Hub
                if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("_HUB_CS")))
                {
                    IotHubOwnerConnectionString = Environment.GetEnvironmentVariable("_HUB_CS");
                    Logger.Information("IoT Hub owner connection string read from environment.");
                }
            }

            Logger.Information($"IoTHub device cert store type is: {IotDeviceCertStoreType}");
            Logger.Information($"IoTHub device cert path is: {IotDeviceCertStorePath}");

            // save the device connectionstring, if we have one
            if (!string.IsNullOrEmpty(IotCentralDeviceConnectionString))
            {
                Logger.Information($"Adding IoT Central device connection string to secure store.");
                SecureIoTHubToken.WriteAsync(ApplicationName, IotCentralDeviceConnectionString, IotDeviceCertStoreType, IotDeviceCertStorePath).Wait();
            }
            else
            {
                string errorMessage = "When running in IoT Hub extended mode you have to pass a connection string with icdc|iotcentraldeviceconnectionstring parameter";
                Logger.Fatal(errorMessage);
                throw new ArgumentException(errorMessage);
            }

            // connect as device client
            if (isIotEdge)
            {
                Logger.Information($"Create IoT Central device client using '{SendHubProtocol}' for communication.");
                IotHubClient = HubClient.CreateDeviceClientFromConnectionString(IotCentralDeviceConnectionString, SendHubProtocol, Logger);
            }
            else
            {
                Logger.Error("Your module must run in IoTEdge mode to run in extended mode.");
            }

            if (!InitHubCommunicationAsync(IotHubClient, false, true).Result)
            {
                string errorMessage = $"Cannot create IoTHub client. Exiting...";
                Logger.Fatal(errorMessage);
                throw new Exception(errorMessage);
            }
        }

        private static readonly object _singletonLockExtension = new object();
        private static IotHubCommunication _instanceExtension = null;
    }
}
