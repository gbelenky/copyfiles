# Copy many large files from one storage account blob container into another storage account blob container

Prerequisites to run it locally:
Two Azure storage accounts - one for the source blob container and another for the desination blob container
 
 [VS Code with extensions](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-create-first-csharp?pivots=code-editor-vscode#prerequisites)

 [Azurite Storage Emulator](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio-code)


This repo contains two directories with two dotnet core durable functions:

1. setup-test-files
2. copy-files

setup-test-files creates n clones of a file in your source container - currently it is hard coded to 50000 iterations. You need to upload the initial file manually.   

I generated a 2MB file
Windows: use cmd prompt (run as administrator) and create a test file:
````
fsutil file createNew gb1.test 2000000
````

and uploaded it with the [Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/)

to run the samples on your machine start Azurite first, run the application and start the durable function client.

After you uploaded the initial file, start your setup-test-files
here you will need the following config params in the local.setting.json:

"SRC_BLOB_STORAGE" :"YOURCONNECTIONSTRING"

"DST_BLOB_STORAGE" :"YOURCONNECTIONSTRING"

to run the copy files after that you will need to add follwing config params into your local.settings.json file :

"AzureWebJobsSRC_BLOB_STORAGE" : "YOURCONNECTIONSTRING"
"AzureWebJobsDST_BLOB_STORAGE" : "YOURCONNECTIONSTRING"




