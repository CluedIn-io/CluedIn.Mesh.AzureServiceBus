using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using CluedIn.Core;
using CluedIn.Core.Data;
using CluedIn.Core.Mesh;
using CluedIn.Core.Messages.Processing;
using CluedIn.Core.Messages.WebApp;

namespace CluedIn.Mesh.AzureServiceBus
{
    public class AzureServiceBusUpdateBaseMeshProcessor : BaseMeshProcessor
    {

        protected AzureServiceBusUpdateBaseMeshProcessor(ApplicationContext appContext)
            : base(appContext)
        {
            
        }

        public override bool Accept(MeshDataCommand command, MeshQuery query, IEntity entity)
        {
            return true;
        }

        public override void DoProcess(CluedIn.Core.ExecutionContext context, MeshDataCommand command, IDictionary<string, object> jobData, MeshQuery query)
        {
            return;
        }

        public override List<RawQuery> GetRawQueries(IDictionary<string, object> config, IEntity entity, Core.Mesh.Properties properties)
        {
            var message = new AzureServiceBusMessage() { Configuration = config, Entity = entity, ChangeSet = properties };

            return new List<Core.Messages.WebApp.RawQuery>()
            {
                new Core.Messages.WebApp.RawQuery()
                {
                    Query = JsonUtility.Serialize(message),
                    Source = "Azure Service Bus Message"
                }
            };
        }

        public override Guid GetProviderId()
        {
            return default(Guid);
        }

        public override string GetVocabularyProviderKey()
        {
            return string.Empty;
        }

        public override string GetLookupId(IEntity entity)
        {
            var code = entity.Codes.First();
            return code.Value;
        }

        public override List<QueryResponse> RunQueries(IDictionary<string, object> config, string id, Core.Mesh.Properties properties)
        {
            return Task.Run(() => PushMessageAsync(config, id, properties)).Result;
        }

        private async Task<List<QueryResponse>> PushMessageAsync(IDictionary<string, object> config, string id, Core.Mesh.Properties properties)
        {
            string connectionString = "<connection_string>";
            string queueName = "<queue_name>";

            await using var client = new ServiceBusClient(connectionString);
            var objectToSend = new AzureServiceBusMessage() { Configuration = config, Entity = AppContext.System.Organization.DataStores.PrimaryDataStore.GetById(AppContext.System.CreateExecutionContext(), new Guid(id)), ChangeSet = properties };
         
            ServiceBusSender sender = client.CreateSender(queueName);
            ServiceBusMessage message = new ServiceBusMessage(JsonUtility.Serialize(objectToSend));

            await sender.SendMessageAsync(message);

            return new List<QueryResponse>() { new QueryResponse() { Content = string.Empty, StatusCode = System.Net.HttpStatusCode.OK } };
        }

        public override List<QueryResponse> Validate(ExecutionContext context, MeshDataCommand command, IDictionary<string, object> config, string id, MeshQuery query)
        {
            return new List<QueryResponse>() { new QueryResponse() { Content = string.Empty, StatusCode = System.Net.HttpStatusCode.OK } };
        }
    }

    public class AzureServiceBusMessage
    {
        public IDictionary<string, object> Configuration { get; set; }

        public Core.Mesh.Properties ChangeSet { get; set; }

        public IEntity Entity { get; set; }
    }
}
