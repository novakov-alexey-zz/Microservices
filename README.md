# Microservices
Http file upload service and FileIO file processor service based on Akka-Streams/Http  
In order to run these service you need to install SBT, preferably 0.13.12 v. or higher

# Start HttpUploadService
sbt "runMain service.upload.HttpUploadService"

#Start DataProcessorService
sbt "runMain service.dataprocessor.DataProcessorService"

#Test Workflow
curl --form "csv=@\<path to CSV file\>" http://localhost:8080/upload/csv  
Please see file CSV file format here: src/test/resources/test.csv