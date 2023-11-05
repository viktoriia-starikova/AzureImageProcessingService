# AzureImageProcessingService
- Image processing service - no public API
    - Changes states in Azure Cosmos DB for this task to "in progress"
    - Read file from Azure Blob Storage in memory, flips it on 180 degrees (upside down)
    - Saves the processed image to Azure Blob Storage
    - Changes state in Azure Cosmos DB to "done" + adds the route to processed file in Azure Blob Storage
