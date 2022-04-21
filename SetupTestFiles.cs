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
        private static long iterations = 2;

        [FunctionName("SetupTestFiles")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger logger)
        {
            string fileNameToClone = await context.CallActivityAsync<string>("GetFileNameToClone", null);
            var tasks = new Task[iterations];
            for (int i = 0; i < iterations; i++)
            {
                tasks[i] = context.CallActivityAsync("CloneFile", $"{fileNameToClone}_{i}");
            }
            await Task.WhenAll(tasks);

            return;
        }

        [FunctionName("CloneFile")]
        [StorageAccount("SRC_BLOB_STORAGE")]
        public static void CloneFile([ActivityTrigger] string blobName,
        [Blob("srcfiles")] BlobContainerClient container, ILogger log)
        {
            // used async sample here - https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-copy?tabs=dotnet#copy-a-blob
            try
            {
                // Create a BlobClient representing the source blob to copy.
                BlobClient sourceBlob = container.GetBlobClient(blobName);

                // Ensure that the source blob exists.
                if (sourceBlob.Exists())
                {
                    // Lease the source blob for the copy operation 
                    // to prevent another client from modifying it.
                    BlobLeaseClient lease = sourceBlob.GetBlobLeaseClient();

                    // Specifying -1 for the lease interval creates an infinite lease.
                    lease.Acquire(TimeSpan.FromSeconds(-1));

                    // Get the source blob's properties and display the lease state.
                    BlobProperties sourceProperties = sourceBlob.GetProperties();
                    Console.WriteLine($"Lease state: {sourceProperties.LeaseState}");

                    // Get a BlobClient representing the destination blob with a unique name.
                    BlobClient destBlob =
                        container.GetBlobClient(Guid.NewGuid() + "-" + sourceBlob.Name);

                    // Start the copy operation.
                    destBlob.StartCopyFromUri(sourceBlob.Uri);

                    // Get the destination blob's properties and display the copy status.
                    BlobProperties destProperties = destBlob.GetProperties();

                    log.LogInformation($"Copy status: {destProperties.CopyStatus}");
                    log.LogInformation($"Copy progress: {destProperties.CopyProgress}");
                    log.LogInformation($"Completion time: {destProperties.CopyCompletedOn}");
                    log.LogInformation($"Total bytes: {destProperties.ContentLength}");

                    // Update the source blob's properties.
                    sourceProperties = sourceBlob.GetProperties();

                    if (sourceProperties.LeaseState == LeaseState.Leased)
                    {
                        // Break the lease on the source blob.
                        lease.Break();

                        // Update the source blob's properties to check the lease state.
                        sourceProperties = sourceBlob.GetProperties();
                        log.LogInformation($"Lease state: {sourceProperties.LeaseState}");
                    }
                }
            }
            catch (RequestFailedException ex)
            {
                log.LogInformation(ex.Message);
                throw;
            }
            log.LogInformation($"Blob {blobName} was created.");
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