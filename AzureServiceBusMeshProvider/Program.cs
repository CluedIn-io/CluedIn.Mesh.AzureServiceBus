using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace AzureServiceBusMeshProvider
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string connectionString = "<connection_string>";
            string queueName = "<queue_name>";

            // Because ServiceBusClient implements IAsyncDisposable, we'll create it 
            // with "await using" so that it is automatically disposed for us.
            await using var client = new ServiceBusClient(connectionString);

            // The sender is responsible for publishing messages to the queue.
            ServiceBusSender sender = client.CreateSender(queueName);
            ServiceBusMessage message = new ServiceBusMessage("Hello world!");

            await sender.SendMessageAsync(message);

            // The receiver is responsible for reading messages from the queue.
            ServiceBusReceiver receiver = client.CreateReceiver(queueName);
            ServiceBusReceivedMessage receivedMessage = await receiver.ReceiveMessageAsync();

            string body = receivedMessage.Body.ToString();
            Console.WriteLine(body);

        }
    }
}
