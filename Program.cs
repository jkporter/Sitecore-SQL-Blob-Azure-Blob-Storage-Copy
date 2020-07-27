using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Data.SqlClient;
using ShellProgressBar;

namespace SitecoreSqlServerToAzureStorageBlobCopy
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var storageAccount = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");

            var options = new ProgressBarOptions
            {
                CollapseWhenFinished = false
            };
            using var progressBar = new ProgressBar(0, "progress bar is on the bottom now", options);
            var blobServiceClient = new BlobServiceClient(storageAccount);
            var blobContainerClient = blobServiceClient.GetBlobContainerClient("blobs");
            await blobContainerClient.CreateIfNotExistsAsync().ConfigureAwait(true);

            var tasks = new List<Task>();

            await foreach (var blob in GetBlobs())
            {
                progressBar.MaxTicks += 1;
                var blobName = blob.Id.ToString();
                // progressBar.Tick($"Copying blob {progressBar.CurrentTick + 1} of {progressBar.MaxTicks}");
                var childProgressBar = progressBar.Spawn((int)blob.Size, blobName, options);
                tasks.Add(CreateBlobAsync(blob.Id, blobContainerClient.GetBlockBlobClient(blobName),
                    childProgressBar.AsProgress<long>(null, l => l / (double)blob.Size)));
            }

            await Task.WhenAll(tasks.ToArray()).ConfigureAwait(true);
        }

        private static async IAsyncEnumerable<(Guid Id, long Size)> GetBlobs([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT BlobId, SUM(DATALENGTH(Data)) FROM Blobs GROUP BY BlobId";
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                yield return (reader.GetGuid(0), reader.GetInt32(1));
        }

        public static string ConnectionString { get; set; }

        private static async Task CreateBlobAsync(Guid id, BlockBlobClient blockBlobClient, IProgress<long> progress,
            CancellationToken cancellationToken = default)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT [Index], Data FROM Blobs ORDER BY [Index] WHERE BlobId = @p0";
            command.Parameters.Add(id);

            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var reader = await command
                .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);

            var base64BlockIds = new List<string>();
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var base64BlockId = Convert.ToBase64String(BitConverter.GetBytes(reader.GetInt32(0)));
                await using var source = reader.GetStream(1);
                await blockBlobClient.StageBlockAsync(base64BlockId, source, null, null, progress, cancellationToken)
                    .ConfigureAwait(false);
                base64BlockIds.Add(base64BlockId);
            }

            await blockBlobClient.CommitBlockListAsync(base64BlockIds, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
