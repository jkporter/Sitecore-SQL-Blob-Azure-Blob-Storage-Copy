using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Data.SqlClient;
using ShellProgressBar;

namespace SitecoreSqlServerToAzureStorageBlobCopy
{
    static class Program
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
            await blobContainerClient.CreateIfNotExistsAsync();

            var tasks = new List<Task>();

            await foreach (var (id, partitions, size) in GetBlobs())
            {
                progressBar.MaxTicks += 1;
                var blobName = id.ToString();
                // progressBar.Tick($"Copying blob {progressBar.CurrentTick + 1} of {progressBar.MaxTicks}");
                var childProgressBar = progressBar.Spawn((int)size, blobName, options);
                tasks.Add(CreateBlockBlobAsync(id, partitions, blobContainerClient.GetBlockBlobClient(blobName),
                    childProgressBar.AsProgress<long>(null, l => l / (double)size)));
            }

            await Task.WhenAll(tasks);
        }

        private static async IAsyncEnumerable<(Guid Id, int Partitions, long Size)> GetBlobs([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT BlobId, MAX([Index]) + 1, SUM(DATALENGTH(Data)) FROM Blobs GROUP BY BlobId";
            await connection.OpenAsync(cancellationToken);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                yield return (reader.GetGuid(0), reader.GetInt32(1), reader.GetInt64(2));
        }

        public static string ConnectionString { get; set; }

        private static async Task CreateBlockBlobAsync(Guid id, int? partitions, BlockBlobClient blockBlobClient , IProgress<long> progress,
            CancellationToken cancellationToken = default)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT [Index], Data FROM Blobs ORDER BY [Index] WHERE BlobId = @p0";
            command.Parameters.Add(id);

            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var reader = await command
                .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

            var base64BlockIds = partitions.HasValue ? new List<string>(partitions.Value) : new List<string>();
            var indexBytes = new byte[4];
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                BinaryPrimitives.WriteInt32BigEndian(indexBytes, reader.GetInt32(0));
                var base64BlockId = Convert.ToBase64String(indexBytes);
                await using var source = reader.GetStream(1);
                await blockBlobClient.StageBlockAsync(base64BlockId, source, null, null, progress, cancellationToken);
                base64BlockIds.Add(base64BlockId);
            }

            await blockBlobClient.CommitBlockListAsync(base64BlockIds, cancellationToken: cancellationToken);
        }

        private static async Task CreateBlobAsync2(Guid id, int count, BlockBlobClient blockBlobClient, IProgress<long> progress,
            CancellationToken cancellationToken = default)
        {
            var base64BlockIds = new ConcurrentDictionary<int, string>();
            await Task.WhenAll(Enumerable.Range(0, count).Select(index =>
                StageBlockAsync(id, index, base64BlockIds, blockBlobClient, progress, cancellationToken)));

            await blockBlobClient
                .CommitBlockListAsync(base64BlockIds.Keys.OrderBy(key => key).Select(key => base64BlockIds[key]),
                    cancellationToken: cancellationToken);
        }

        private static async Task StageBlockAsync(Guid id, int index, ConcurrentDictionary<int, string> base64BlockIds, BlockBlobClient blockBlobClient, IProgress<long> progress,
            CancellationToken cancellationToken = default)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT Data FROM Blobs WHERE BlobId = @p0 AND [Index] = @p1";
            command.Parameters.Add(id);
            command.Parameters.Add(index);

            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var reader = await command
                .ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, cancellationToken);

            await reader.ReadAsync(cancellationToken);

            var indexBytes = MemoryPool<byte>.Shared.Rent(4).Memory[..4];
            BinaryPrimitives.WriteInt32BigEndian(indexBytes.Span, reader.GetInt32(0));
            var base64BlockId = Convert.ToBase64String(indexBytes.Span);
            await using var source = reader.GetStream(1);
            await blockBlobClient.StageBlockAsync(base64BlockId, source, null, null, progress, cancellationToken);

            base64BlockIds[index] = base64BlockId;
        }
    }
}
