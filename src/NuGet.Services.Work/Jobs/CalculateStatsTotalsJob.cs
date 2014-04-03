using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NuGet.Indexing;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Jobs.Bases;
using NuGet.Services.Work.Monitoring;

namespace NuGet.Services.Work.Jobs
{
    public class CalculateStatsTotalsJob : JobHandler<CaclculateStatsTotalsEventSource>
    {
        /// <summary>
        /// Gets or sets an Azure Storage account with the container for the content blobs
        /// </summary>
        public CloudStorageAccount ContentAccount { get; set; }
        public string ContentContainer { get; set; }

        /// <summary>
        /// Gets or set a connection string to the database containing package data.
        /// </summary>
        public SqlConnectionStringBuilder PackageDatabase { get; set; }

        protected StorageHub Storage { get; set; }
        protected ConfigurationHub Config { get; set; }

        // Note the NOLOCK hints here!
        public static readonly string GetStatisticsSql = @"SELECT 
                    (SELECT COUNT([Key]) FROM PackageRegistrations pr WITH (NOLOCK)
                            WHERE EXISTS (SELECT 1 FROM Packages p WITH (NOLOCK) WHERE p.PackageRegistrationKey = pr.[Key] AND p.Listed = 1)) AS UniquePackages,
                    (SELECT COUNT([Key]) FROM Packages WITH (NOLOCK) WHERE Listed = 1) AS TotalPackages,
                    (SELECT TotalDownloadCount FROM GallerySettings WITH (NOLOCK)) AS DownloadCount";

        public CalculateStatsTotalsJob(StorageHub storage, ConfigurationHub config)
        {
            Storage = storage;
            Config = config;
        }

        protected internal override async Task Execute()
        {
            Log.BeginningQuery();
            ContentAccount = ContentAccount ?? Storage.Legacy.Account;
            ContentContainer = Source.CreateCloudBlobClient().GetContainerReference(
                Strings.IsNullOrEmpty(ContentContainer)) ? "content" : ContentContainer);
            
            Totals totals;
            Log.BeginningQuery(PackageDatabase.DataSource, PackageDatabase.InitialCatalog);
            using (var connection = await PackageDatabase.ConnectTo())
            {
                totals = (await connection.QueryAsync<Totals>(GetStatisticsSql)).SingleOrDefault();
            }
            Log.FinishedQuery(totals.UniquePackages, totals.TotalPackages, totals.DownloadCount)

            string name = "stats-totals.json";
            Log.BeginningBlobUpload(name);
            await Storage.Primary.Blobs.UploadJsonBlob(totals, ContentContainer, name);
            Log.FinishedBlobUpload();
        }

        public class Totals
        {
            public int UniquePackages { get; set; }
            public int TotalPackages { get; set; }
            public int DownloadCount { get; set; }
        }
    }

    [EventSource(Name="Outercurve-NuGet-Jobs-CalculateStatsTotals")]
    public class CaclculateStatsTotalsEventSource : EventSource
    {
        public static readonly CaclculateStatsTotalsEventSource Log = new CaclculateStatsTotalsEventSource();
        private CaclculateStatsTotalsEventSource() { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Begining the query of the database to get statistics from {0}/{1}",
            Task = Tasks.Querying,
            Opcode = EventOpcode.Start)]
        public void BeginningQuery(string server, string database) { WriteEvent(1, server, database); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Message = "Finished querying the database. Unique Packages: {0}, Total Packages: {1}, Download Count: {2}",
            Task = Tasks.Querying,
            Opcode = EventOpcode.Stop)]
        public void FinishedQuery(int uniquePackages, int totalPackages, int downloadCount)
        {
            WriteEvent(2, uniquePackages, totalPackages, downloadCount);
        }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Message = "Beginning blob upload: {0}",
            Task = Tasks.Uploading,
            Opcode = EventOpcode.Start)]
        public void BeginningBlobUpload(string blobName) { WriteEvent(3, blobName); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Message = "Finished blob upload",
            Task = Tasks.Uploading,
            Opcode = EventOpcode.Stop)]
        public void FinishedBlobUpload() { WriteEvent(4); }

        public static class Tasks
        {
            public const EventTask Querying = (EventTask)0x1;
            public const EventTask Uploading = (EventTask)0x2;
        }
    }
}
