# Copy 50k files of average size of 2MB each from one storage account blob container into another storage account blob container

## prerequisites - a generated 2MB file
Windows: use cmd prompt (run as administrator) and create a test file:
````
fsutil file createNew gb1.test 2000000
````


## setting up test files
1. manually upload first generated file to the BLOB storage
2. Clone first file 49.999 times in the source storage account blob container

## running test
1. Copy all files from the source storage account blob container to the destination storage account blob container
2. Delete all files in the source storage account blob container

