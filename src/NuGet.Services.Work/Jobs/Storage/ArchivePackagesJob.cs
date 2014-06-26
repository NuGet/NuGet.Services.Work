using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json.Linq;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Jobs.Models;

namespace NuGet.Services.Work.Jobs
{
    [Description("Creates an archive of packages based on information in the NuGet API v2 Database")]
    public class ArchivePackagesJob : JobHandler<ArchivePackagesEventSource>
    {
        private const string DefaultCursorBlob = "cursor.json";
        private const string ContentTypeJson = "application/json";
        private const string DateTimeFormatSpecifier = "O";
        private const string LastPublishedKey = "lastPublished";
        private const string LastLastEditedKey = "lastLastEdited";

        /// <summary>
        /// Gets or sets an Azure Storage Uri referring to a container to use as the source for package blobs
        /// </summary>
        public CloudStorageAccount Source { get; set; }
        public string SourceContainerName { get; set; }

        /// <summary>
        /// Gets or sets an Azure Storage Uri referring to a container to use as the destination
        /// </summary>
        public CloudStorageAccount PrimaryDestination { get; set; }

        /// <summary>
        /// Gets or sets an Azure Storage Uri referring to a container to use as the secondary destination
        /// DestinationContainerName should be same as the primary destination
        /// </summary>
        public CloudStorageAccount SecondaryDestination { get; set; }
        /// <summary>
        /// Destination Container name for both Primary and Secondary destinations. Also, for the cursor blob
        /// </summary>
        public string DestinationContainerName { get; set; }

        /// <summary>
        /// Blob containing the cursor data. Cursor data comprises of LastPublished and LastLastEdited
        /// </summary>
        public string CursorBlob { get; set; }

        /// <summary>
        /// Gets or sets a connection string to the database containing package data.
        /// </summary>
        public SqlConnectionStringBuilder PackageDatabase { get; set; }

        protected ConfigurationHub Config { get; private set; }

        protected CloudBlobContainer SourceContainer { get; private set; }
        protected CloudBlobContainer PrimaryDestinationContainer { get; private set; }
        protected CloudBlobContainer SecondaryDestinationContainer { get; private set; }

        public ArchivePackagesJob(ConfigurationHub config)
        {
            Config = config;
        }

        private async Task<JObject> GetJObject(CloudBlobContainer container, string blobName)
        {
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            string json = await blob.DownloadTextAsync();
            return JObject.Parse(json);
        }

        private async Task SetJObject(CloudBlobContainer container, string blobName, JObject jObject)
        {
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            blob.Properties.ContentType = ContentTypeJson;
            await blob.UploadTextAsync(jObject.ToString());
        }

        protected internal override async Task Execute()
        {
            // Configure defaults as needed
            PackageDatabase = PackageDatabase ?? Config.Sql.GetConnectionString(KnownSqlConnection.Legacy);
            Source = Source ?? Config.Storage.Legacy;
            PrimaryDestination = PrimaryDestination ?? Config.Storage.Legacy;
            SecondaryDestination = SecondaryDestination ?? Config.Storage.Backup;

            SourceContainer = Source.CreateCloudBlobClient().GetContainerReference(
                String.IsNullOrEmpty(SourceContainerName) ? BlobContainerNames.LegacyPackages : SourceContainerName);
            PrimaryDestinationContainer = PrimaryDestination.CreateCloudBlobClient().GetContainerReference(
                String.IsNullOrEmpty(DestinationContainerName) ? BlobContainerNames.Backups : DestinationContainerName);
            SecondaryDestinationContainer = SecondaryDestination == null ? null : SecondaryDestination.CreateCloudBlobClient().GetContainerReference(
                String.IsNullOrEmpty(DestinationContainerName) ? BlobContainerNames.Backups : DestinationContainerName);
            CursorBlob = String.IsNullOrEmpty(CursorBlob) ? DefaultCursorBlob : CursorBlob;

            Log.PreparingToArchive(Source.Credentials.AccountName, SourceContainer.Name, PrimaryDestination.Credentials.AccountName, PrimaryDestinationContainer.Name, PackageDatabase.DataSource, PackageDatabase.InitialCatalog);
            await Archive(PrimaryDestination, PrimaryDestinationContainer);

            if (SecondaryDestinationContainer != null)
            {
                Log.PreparingToArchive2(SecondaryDestination.Credentials.AccountName, SecondaryDestinationContainer.Name);
                await Archive(SecondaryDestination, SecondaryDestinationContainer);
            }
        }

        private async Task Archive(CloudStorageAccount destination, CloudBlobContainer destinationContainer)
        {
            var cursorJObject = await GetJObject(destinationContainer, CursorBlob);
            var lastPublished = cursorJObject.Value<DateTime>(LastPublishedKey);
            var lastLastEdited = cursorJObject.Value<DateTime>(LastLastEditedKey);

            Log.CursorData(lastPublished.ToString(DateTimeFormatSpecifier), lastLastEdited.ToString(DateTimeFormatSpecifier));

            Log.GatheringNewDBPackages(PackageDatabase.DataSource, PackageDatabase.InitialCatalog);
            IList<PackageRef> packages;
            using (var connection = await PackageDatabase.ConnectTo())
            {
                packages = (await connection.QueryAsync<PackageRef>(@"
                    SELECT pr.Id, p.NormalizedVersion AS Version, p.Hash
                    FROM Packages p
                    INNER JOIN PackageRegistrations pr ON p.PackageRegistrationKey = pr.[Key]"))
                    .ToList();
            }
            
        }

        private async Task BackupPackage(string sourceBlobName, string destinationBlobName, CloudBlobContainer destinationContainer)
        {
            // Identify the source and destination blobs
            var sourceBlob = SourceContainer.GetBlockBlobReference(sourceBlobName);
            var destBlob = destinationContainer.GetBlockBlobReference(destinationBlobName);

            if (await destBlob.ExistsAsync())
            {
                Log.ArchiveExists(destBlob.Name);
            }
            else if (!await sourceBlob.ExistsAsync())
            {
                Log.SourceBlobMissing(sourceBlob.Name);
            }
            else
            {
                // Start the copy
                Log.StartingCopy(sourceBlob.Name, destBlob.Name);
                if (!WhatIf)
                {
                    await destBlob.StartCopyFromBlobAsync(sourceBlob);
                }
                Log.StartedCopy(sourceBlob.Name, destBlob.Name);
            }
        }
    }

    [EventSource(Name="Outercurve-NuGet-Jobs-ArchivePackages")]
    public class ArchivePackagesEventSource : EventSource
    {
        public static readonly ArchivePackagesEventSource Log = new ArchivePackagesEventSource();

        private ArchivePackagesEventSource() { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Preparing to archive packages from {0}/{1} to primary destination {2}/{3} using package data from {4}/{5}")]
        public void PreparingToArchive(string sourceAccount, string sourceContainer, string destAccount, string destContainer, string dbServer, string dbName) { WriteEvent(1, sourceAccount, sourceContainer, destAccount, destContainer, dbServer, dbName); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Message = "Preparing to archive packages to secondary destination {0}/{1}")]
        public void PreparingToArchive2(string destAccount, string destContainer) { WriteEvent(2, destAccount, destContainer); }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Message = "Cursor data: LastPublished is {0}, LastLastEdited is {1}")]
        public void CursorData(string lastPublished, string lastLastEdited) { WriteEvent(3, lastPublished, lastLastEdited); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Task = Tasks.GatheringDBPackages,
            Opcode = EventOpcode.Start,
            Message = "Gathering list of new packages from {0}/{1}")]
        public void GatheringNewDBPackages(string dbServer, string dbName) { WriteEvent(4, dbServer, dbName); }

        [Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Task = Tasks.GatheringDBPackages,
            Opcode = EventOpcode.Stop,
            Message = "Gathered {0} new packages from {1}/{2}")]
        public void GatheredNewDBPackages(int gathered, string dbServer, string dbName) { WriteEvent(5, gathered, dbServer, dbName); }

        [Event(
            eventId: 6,
            Level = EventLevel.Informational,
            Task = Tasks.GatheringDBPackages,
            Opcode = EventOpcode.Start,
            Message = "Gathering list of edited packages from {0}/{1}")]
        public void GatheringEditedDBPackages(string dbServer, string dbName) { WriteEvent(6, dbServer, dbName); }

        [Event(
            eventId: 7,
            Level = EventLevel.Informational,
            Task = Tasks.GatheringDBPackages,
            Opcode = EventOpcode.Stop,
            Message = "Gathered {0} edited packages from {1}/{2}")]
        public void GatheredEditedDBPackages(int gathered, string dbServer, string dbName) { WriteEvent(7, gathered, dbServer, dbName); }

        [Event(
            eventId: 8,
            Level = EventLevel.Informational,
            Message = "Archive already exists: {0}")]
        public void ArchiveExists(string blobName) { WriteEvent(8, blobName); }

        [Event(
            eventId: 9,
            Level = EventLevel.Warning,
            Message = "Source Blob does not exist: {0}")]
        public void SourceBlobMissing(string blobName) { WriteEvent(9, blobName); }

        [Event(
            eventId: 12,
            Level = EventLevel.Informational,
            Task = Tasks.StartingPackageCopy,
            Opcode = EventOpcode.Start,
            Message = "Starting copy of {0} to {1}.")]
        public void StartingCopy(string source, string dest) { WriteEvent(12, source, dest); }

        [Event(
            eventId: 13,
            Level = EventLevel.Informational,
            Task = Tasks.StartingPackageCopy,
            Opcode = EventOpcode.Stop,
            Message = "Started copy of {0} to {1}.")]
        public void StartedCopy(string source, string dest) { WriteEvent(13, source, dest); }
    }

    public static class Tasks
    {
        public const EventTask GatheringDBPackages = (EventTask)0x1;
        public const EventTask StartingPackageCopy = (EventTask)0x2;
    }
}
