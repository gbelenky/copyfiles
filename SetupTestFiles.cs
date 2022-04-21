using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using System.Linq;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs.Models;


namespace gbelenky.Storage
{
    public static class SetupTestFiles
    {
        private static long iterations = 2;

        [FunctionName("SetupTestFiles")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger logger)
        {
            string fileNameToClone = await context.CallActivityAsync<string>("GetFileNameToClone", "someValue");
            var tasks = new Task[iterations];
            for (int i = 0; i < iterations; i++)
            {
                tasks[i] = context.CallActivityAsync("CloneFile", fileNameToClone + i);
            }
            await Task.WhenAll(tasks);

            return;
        }

        [FunctionName("CloneFile")]
        public static string CloneFile([ActivityTrigger] string name,
        [Blob("srcfiles")] BlobContainerClient blobContainerClient, ILogger log)
        {
            log.LogInformation($"Saying hello to {name}.");
            return $"Hello {name}!";
        }

        [FunctionName("GetFileNameToClone")]
        [StorageAccount("SRC_BLOB_STORAGE")]
        public static string GetFileNameToClone([ActivityTrigger] string someValue,
        [Blob("srcfiles")] BlobContainerClient blobContainerClient, ILogger log)

        {
            string fileName = blobContainerClient.GetBlobs().FirstOrDefault().Name;
            log.LogInformation($"GetFileNameToClone - {fileName}");
            return $"{fileName}";
        }


        [FunctionName("SetupTestFiles_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("SetupTestFiles", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}