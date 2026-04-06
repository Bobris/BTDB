using System;

namespace BTDB.AzureStorage;

public interface IAzureBlobFileCollectionLogger
{
    void InitialDownloadExecuting(string blobName, int remainingDownloads, long fileLength)
    {
    }

    void OperationQueued(string operation, int queueLength)
    {
    }

    void OperationExecuting(string operation, int queueLength, long? fileLength = null)
    {
    }

    void OperationFailed(string operation, int queueLength, Exception exception)
    {
    }
}
