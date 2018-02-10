using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using ShellProgressBar;

namespace SitecoreSqlServerToAzureStorageBlobCopy
{
    class Program
    {
        static async Task Main(string[] args)
        {
            const string connectionString = "data source=DESKTOP-0LFEV64\\SQLEXPRESS;initial catalog=TestInstanceSitecore_Master;integrated security=True;MultipleActiveResultSets=True";

            var storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");

            var blobClient = storageAccount.CreateCloudBlobClient();
            var blobContainer = blobClient.GetContainerReference("blobs");
            await blobContainer.CreateIfNotExistsAsync();

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                var counts = new Dictionary<Guid, long>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT BlobId, SUM(DATALENGTH(Data)) FROM Blobs GROUP BY BlobId";
                    using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess))
                    {
                        while (await reader.ReadAsync())
                        {
                            counts.Add(reader.GetGuid(0), reader.GetInt32(1));
                        }
                    }
                }

                using (var command = connection.CreateCommand())
                {

                    var options = new ProgressBarOptions
                    {
                        CollapseWhenFinished = false
                    };
                    using (var progressBar = new ProgressBar(counts.Count, "progress bar is on the bottom now", options))
                    {
                        command.CommandText = "SELECT BlobId, Data FROM Blobs ORDER BY BlobId, [Index]";


                        Guid? currentBlobId = null;
                        Stream cloubBlobStream = null;
                        ChildProgressBar childProgressBar = null;

                        var progress = new Progress<long>();
                        long bytesWritten = 0;
                        progress.ProgressChanged += (sender, l) =>
                        {
                            bytesWritten += l;
                            childProgressBar.Tick((int)bytesWritten); 

                        };

                        using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess))
                        {
                            while (await reader.ReadAsync())
                            {
                                var blobId = reader.GetGuid(0);
                                if (blobId != currentBlobId)
                                {
                                    cloubBlobStream?.Close();
                                    childProgressBar?.Dispose();
                                    bytesWritten = 0;

                                    progressBar.Tick($"Copying blob {progressBar.CurrentTick+1} of {progressBar.MaxTicks}");
                                    childProgressBar = progressBar.Spawn((int) counts[blobId], blobId.ToString(), options);
                                    cloubBlobStream = await blobContainer
                                        .GetBlockBlobReference((currentBlobId = blobId).ToString()).OpenWriteAsync();
                                }

                                using (var dataStream = reader.GetStream(1))
                                {
                                    await Copy(dataStream, cloubBlobStream, progress);
                                }

                            }
                        }

                        cloubBlobStream?.Close();
                        childProgressBar?.Dispose();
                    }
                }
            }
        }


        private static async Task Copy(Stream source, Stream destination, IProgress<long> progress)
        {
            var buffer = new byte[16 * 1024];

            long bytesWritten = 0;
            int read;
            while ((read = source.Read(buffer, 0, buffer.Length)) != 0)
            {
                await destination.WriteAsync(buffer, 0, read);
                bytesWritten += read;
                progress.Report(bytesWritten);
            }
        }
    }
}
