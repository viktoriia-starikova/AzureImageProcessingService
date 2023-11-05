using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Amqp.Framing;
using System.Drawing;
using System.Security.Cryptography;
using System.Collections.Generic;
using System.Linq;

namespace ImageProcessingService
{
    public class FlipImage
    {
        private Container container;

        // The Cosmos client instance
        private CosmosClient cosmosClient;

        // The database we will create
        private Database database;

        private readonly string databaseId = "Images";
        private readonly string containerId = "TaskState";

        public FlipImage()
        {
            SetUpCosmosClient();
        }

        [FunctionName("RotateImg")]
        public async Task<string> RotateImg(
           [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            try
            {
                await CreateDatabaseAsync();
                await CreateContainerAsync();

                string taskId = req.GetQueryParameterDictionary()["id"];
                TaskState task = await GetCosmosDBItemAsync(taskId);
                await UpdateImageState(task.TaskId, "In progress", string.Empty);

                string url = await RotateImage(task.FileName);
                var updatedTask = await UpdateImageState(task.TaskId, "Done", url);
                return updatedTask.ProcessedFilePath;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private void SetUpCosmosClient()
        {
            string CosmosDBEndpoint = Environment.GetEnvironmentVariable("CosmosDBEndpoint");
            string CosmosDBKey = Environment.GetEnvironmentVariable("CosmosDBKey");

            this.cosmosClient = new CosmosClient(CosmosDBEndpoint, CosmosDBKey, new CosmosClientOptions() { ApplicationName = "CosmosDBDotnetQuickstart" });
        }

        private BlobContainerClient GetBlobContainer()
        {
            string connection = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            string containerName = Environment.GetEnvironmentVariable("ContainerName");

            return new BlobContainerClient(connection, containerName);
        }

        private async Task<string> RotateImage(string fileName)
        {
            BlobContainerClient blobContainer = GetBlobContainer();
            BlobClient blobClient = blobContainer.GetBlobClient(fileName);

            string tempPath = Path.GetTempPath();
            string filePath = Path.Combine(tempPath, fileName);
            await blobClient.DownloadToAsync(filePath);

            var extension = fileName[(fileName.LastIndexOf('.') + 1)..];
            var name = fileName[..fileName.LastIndexOf('.')];
            var newFileName = name + "_flipped." + extension;
            
            BlobClient newBlobClient = blobContainer.GetBlobClient(newFileName);
            using (Image image = Image.FromFile(filePath))
            {
                filePath = Path.Combine(tempPath, newFileName);
                image.RotateFlip(RotateFlipType.Rotate180FlipNone);
                image.Save(filePath);

                await newBlobClient.UploadAsync(filePath);
            }

            return newBlobClient.Uri.ToString();
        }

        private async Task<TaskState> GetCosmosDBItemAsync(string id)
        {
            var sqlQueryText = $"SELECT * FROM c WHERE c.id = '{id}'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<TaskState> queryResultSetIterator = this.container.GetItemQueryIterator<TaskState>(queryDefinition);

            List<TaskState> tasks = new List<TaskState>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<TaskState> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (TaskState task in currentResultSet)
                {
                    tasks.Add(task);
                    Console.WriteLine("\tRead {0}\n", task);
                }
            }
            return tasks.First();
        }

        private async Task CreateDatabaseAsync()
        {
            // Create a new database
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine("Created Database: {0}\n", this.database.Id);
        }

        private async Task CreateContainerAsync()
        {
            // Create a new container
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, "/id");
            Console.WriteLine("Created Container: {0}\n", this.container.Id);
        }

        private async Task<TaskState> UpdateImageState(string id, string state, string url)
        {
            ItemResponse<TaskState> response = await container.PatchItemAsync<TaskState>(
                id: id,
                partitionKey: new PartitionKey(id),
                patchOperations: new[] {
                    PatchOperation.Replace("/State", state),
                    PatchOperation.Replace("/ProcessedFilePath", url),
                }
            );

            TaskState updated = response.Resource;
            return updated;
        }
    }
}
