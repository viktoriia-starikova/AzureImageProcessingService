using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using System.Drawing;
using System.Collections.Generic;
using System.Linq;
using SkiaSharp;

namespace ImageProcessingService
{
    public class FlipImage
    {
        private Container _container;

        // The Cosmos client instance
        private CosmosClient _cosmosClient;
        private Database _database;

        // Blob Storage
        private const string DATABASE_ID = "Images";
        private const string CONTAINER_ID = "TaskState";

        private readonly string COSMOSDB_ENDPOINT = Environment.GetEnvironmentVariable("CosmosDBEndpoint");
        private readonly string COSMOSDB_KEY = Environment.GetEnvironmentVariable("CosmosDBKey");
        private readonly string BLOB_CONNECTION_STRING = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
        private readonly string BLOB_CONTAINER_NAME = Environment.GetEnvironmentVariable("ContainerName");
        private readonly ILogger<FlipImage> _logger;

        public FlipImage(ILogger<FlipImage> log)
        {
            _logger = log;
        }

        [FunctionName("RotateImg")]
        public async Task<string> RotateImg(
           [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            try
            {
                using CosmosClient cosmosClient = await SetUpCosmosClient();

                string taskId = req.GetQueryParameterDictionary()["id"];
                TaskState task = await GetCosmosDBItemAsync(taskId);
                await UpdateImageState(task.TaskId, "In progress", string.Empty);

                string url = await RotateImage(task.FileName);
                TaskState updatedTask = await UpdateImageState(task.TaskId, "Done", url);
                return updatedTask.ProcessedFilePath;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message, ex);
                throw;
            }
        }

        private async Task<CosmosClient> SetUpCosmosClient()
        {
            _cosmosClient = new CosmosClient(COSMOSDB_ENDPOINT, COSMOSDB_KEY, new CosmosClientOptions() { ApplicationName = "CosmosDBDotnetQuickstart" });

            await CreateDatabaseAsync();
            await CreateContainerAsync();

            return _cosmosClient;
        }

        private BlobContainerClient GetBlobContainer()
        {
            return new BlobContainerClient(BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME);
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

            using (var stream = File.OpenRead(filePath))
            using (var originalBitmap = SKBitmap.Decode(stream))
            {
                var flippedBitmap = FlipImageSk(originalBitmap);
                filePath = Path.Combine(tempPath, newFileName);
                using var outputStream = File.Create(filePath);
                flippedBitmap.Encode(SKEncodedImageFormat.Jpeg, 100).SaveTo(outputStream);
            }

            await newBlobClient.UploadAsync(filePath);
            return newBlobClient.Uri.ToString();
        }

        static SKBitmap FlipImageSk(SKBitmap originalBitmap)
        {
            var flippedBitmap = new SKBitmap(originalBitmap.Info);

            using (var canvas = new SKCanvas(flippedBitmap))
            {
                    canvas.Scale(-1, 1, originalBitmap.Width / 2f, 0);

                canvas.DrawBitmap(originalBitmap, 0, 0);
            }

            return flippedBitmap;
        }

        private async Task<TaskState> GetCosmosDBItemAsync(string id)
        {
            var sqlQueryText = $"SELECT * FROM c WHERE c.id = '{id}'";

            _logger.LogInformation("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<TaskState> queryResultSetIterator = _container.GetItemQueryIterator<TaskState>(queryDefinition);

            List<TaskState> tasks = new List<TaskState>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<TaskState> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (TaskState task in currentResultSet)
                {
                    tasks.Add(task);
                    _logger.LogInformation("\tRead {0}\n", task);
                }
            }

            return tasks.First();
        }

        private async Task CreateDatabaseAsync()
        {
            // Create a new database
            _database = await _cosmosClient.CreateDatabaseIfNotExistsAsync(DATABASE_ID);
            _logger.LogInformation($"Created Database: {_database.Id}\n");
        }

        private async Task CreateContainerAsync()
        {
            // Create a new container
            _container = await _database.CreateContainerIfNotExistsAsync(CONTAINER_ID, "/id");
            _logger.LogInformation($"Created Container: {_container.Id}\n");
        }

        private async Task<TaskState> UpdateImageState(string id, string state, string url)
        {
            ItemResponse<TaskState> response = await _container.PatchItemAsync<TaskState>(
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
