using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Azure.Storage.Blobs.Models;
using Azure;
using System;
using Azure.Storage.Blobs.Specialized;

namespace gbelenky.Storage
{
    public static class CopyFiles
    {

        [FunctionName("CopyFiles")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger logger)
        {
            List<string> blobs = await context.CallActivityAsync<List<string>>("GetFileListToCopy", null);

            var tasks = new Task[blobs.Count];
            long i = 0;

            foreach (string blobName in blobs)
            {
                tasks[i] = context.CallActivityAsync("CopyFile", blobName);
                i++;
            }

            await Task.WhenAll(tasks);
            return;
        }

        [FunctionName("CopyFile")]
        [StorageAccount("SRC_BLOB_STORAGE")]
        public static async Task CopyFile([ActivityTrigger] string blobName,
        [Blob("srcfiles", Connection = "SRC_BLOB_STORAGE")] BlobContainerClient srcContainer,
        [Blob("destfiles", Connection = "DST_BLOB_STORAGE")] BlobContainerClient destContainer,
        ILogger log)
        {
            // used async sample here - https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-copy?tabs=dotnet#copy-a-blob
            try
            {
                // Create a BlobClient representing the source blob to copy.
                // get the original name
                BlobClient sourceBlob = srcContainer.GetBlobClient(blobName);
                // Ensure that the source blob exists.
                if (sourceBlob.Exists())
                {
                    // Check the source file's metadata
                    Response<BlobProperties> propertiesResponse = sourceBlob.GetProperties();
                    BlobProperties properties = propertiesResponse.Value;

                    Uri srcBlobUri = GetServiceSasUriForBlob(sourceBlob, log);

                    BlobClient destBlob = destContainer.GetBlobClient(blobName);

                    // Start the copy operation.
                    var ops = destBlob.StartCopyFromUri(srcBlobUri);
                    // Get the destination blob's properties and display the copy status.
                    while (ops.HasCompleted == false)
                    {
                        long copied = await ops.WaitForCompletionAsync();

                        log.LogInformation($"Blob: {destBlob.Name}, Copied: {copied} of {properties.ContentLength}");
                        await Task.Delay(500);
                    }

                    log.LogInformation($"Blob: {destBlob.Name} Complete");

                    // Remove the source blob
                    bool blobExisted = sourceBlob.DeleteIfExists();
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

        private static Uri GetServiceSasUriForBlob(BlobClient blobClient, ILogger log,
            string storedPolicyName = null)
        {
            // Check whether this BlobClient object has been authorized with Shared Key.
            if (blobClient.CanGenerateSasUri)
            {
                // Create a SAS token that's valid for one hour.
                BlobSasBuilder sasBuilder = new BlobSasBuilder()
                {
                    BlobContainerName = blobClient.GetParentBlobContainerClient().Name,
                    BlobName = blobClient.Name,
                    Resource = "b"
                };

                if (storedPolicyName == null)
                {
                    sasBuilder.ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(5);
                    sasBuilder.SetPermissions(BlobSasPermissions.Read |
                        BlobSasPermissions.Write);
                }
                else
                {
                    sasBuilder.Identifier = storedPolicyName;
                }

                Uri sasUri = blobClient.GenerateSasUri(sasBuilder);
                log.LogInformation("SAS URI for blob is: {0}", sasUri);
                return sasUri;
            }
            else
            {
                log.LogInformation(@"BlobClient must be authorized with Shared Key 
                          credentials to create a service SAS.");
                return null;
            }
        }


        [FunctionName("GetFileListToCopy")]
        [StorageAccount("SRC_BLOB_STORAGE")]
        // binding to context because there are no parameters for this function
        public static async Task<List<string>> GetFileListToCopy([ActivityTrigger] IDurableActivityContext context,
        [Blob("srcfiles")] BlobContainerClient blobContainerClient, ILogger log)

        {
            List<string> resultList = new List<string>();
            try
            {
                // Call the listing operation and return pages of the specified size.
                var resultSegment = blobContainerClient.GetBlobsAsync()
                    .AsPages(default, 1000);

                // Enumerate the blobs returned for each page.
                await foreach (Azure.Page<BlobItem> blobPage in resultSegment)
                {
                    foreach (BlobItem blobItem in blobPage.Values)
                    {
                        log.LogInformation("Blob name: {0}", blobItem.Name);
                        resultList.Add(blobItem.Name);
                    }
                }
            }
            catch (RequestFailedException e)
            {
                log.LogInformation(e.Message);
                throw;
            }

            return resultList;
        }

        [FunctionName("CopyFiles_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("CopyFiles", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}
