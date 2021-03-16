using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.SqlServer.XEvent.XELite;

namespace sqlaudit
{
    public class Function1
    {
        private readonly IConfiguration configuration;
        private readonly ICollection<IXEvent> eventCollection;
        public Function1(IConfiguration configuration)
        {
            this.configuration = configuration;
            this.eventCollection = new Collection<IXEvent>();
        }

        [FunctionName("BlobTriggerSqlAuditParser")]
        public async Task Run([BlobTrigger("sqlauditraw/{name}", Connection = "StorageConnectionAppSetting")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            string connectionString = configuration["EventHubConnectionString"];
            string eventHubName = configuration["EventHubName"];

                XEFileEventStreamer xeStream = new XEFileEventStreamer(myBlob);

                var producerClient = new EventHubProducerClient(connectionString, eventHubName);

                await xeStream.ReadEventStream(
                    () =>
                    {
                        Console.WriteLine("Headers found");
                        return Task.CompletedTask;
                    },
                    async xevent =>
                    {
                        await BatchSend(name, xevent, producerClient, eventCollection, log);
                    },
                    CancellationToken.None);
            await BatchSend(name, null, producerClient, eventCollection, log);
        }

        private async Task BatchSend(string blobName, IXEvent xevent, EventHubProducerClient producerClient, ICollection<IXEvent> eventCollection, ILogger log)
        {
            if (xevent != null)
            {
                eventCollection.Add(xevent);
            }
            
            if (eventCollection.Count > 500000 || xevent==null)
            {
                if (xevent == null) log.LogInformation("Processing last batch.");
                // Create a producer client that you can use to send events to an event hub

                // Create a batch of events 
                EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

                foreach (var eventItem in eventCollection)
                {
                    var json = Newtonsoft.Json.JsonConvert.SerializeObject((blobName, eventItem));
                    // Add events to the batch. An event is a represented by a collection of bytes and metadata. 
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(json)));
                }
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                log.LogInformation($"Batch of size: {eventCollection.Count} was sent.");
                eventCollection.Clear();
            }
        }
    }
}
