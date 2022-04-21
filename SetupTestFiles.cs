using System;
using Azure;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using System.Linq;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;


namespace gbelenky.Storage
{
    public static class SetupTestFiles
    {
        private static long iterations = 50000;

        [FunctionName("SetupTestFiles")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger logger)
        {
            string fileNameToClone = await context.CallActivityAsync<string>("GetFileNameToClone", null);
            var tasks = new Task[iterations];
            for (int i = 0; i < iterations; i++)
            {
                string newFileName = $"{fileNameToClone}_{i}";
                tasks[i] = context.CallActivityAsync("CloneFile", newFileName);
            }
            await Task.WhenAll(tasks);

            return;
        }

        [FunctionName("CloneFile")]
        [StorageAccount("SRC_BLOB_STORAGE")]
        public static void CloneFile([ActivityTrigger] string newBlobName, 
        [Blob("srcfiles")] BlobContainerClient container, ILogger log)
        {
            // used async sample here - https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-copy?tabs=dotnet#copy-a-blob
            try
            {
                // Create a BlobClient representing the source blob to copy.
                string blobName = newBlobName.Split('_',2)[0];
                BlobClient sourceBlob = container.GetBlobClient(blobName);

                // Ensure that the source blob exists.
                if (sourceBlob.Exists())
                {
                    // Get a BlobClient representing the destination blob with a unique name.
                    BlobClient destBlob =
                        container.GetBlobClient(newBlobName);

                    // Start the copy operation.
                    destBlob.StartCopyFromUri(sourceBlob.Uri);

                    // Get the destination blob's properties and display the copy status.
                    BlobProperties destProperties = destBlob.GetProperties();

                    log.LogInformation($"Copy status: {destProperties.CopyStatus}");
                    log.LogInformation($"Copy progress: {destProperties.CopyProgress}");
                    log.LogInformation($"Completion time: {destProperties.CopyCompletedOn}");
                    log.LogInformation($"Total bytes: {destProperties.ContentLength}");
                }
            }
            catch (RequestFailedException ex)
            {
                log.LogInformation(ex.Message);
                throw;
            }
            log.LogInformation($"Blob {newBlobName} was created.");
            return;
        }

        [FunctionName("GetFileNameToClone")]
        [StorageAccount("SRC_BLOB_STORAGE")]
        // binding to context because there are no parameters for this function
        public static string GetFileNameToClone([ActivityTrigger] IDurableActivityContext context,
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