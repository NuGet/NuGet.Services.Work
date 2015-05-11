// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
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
        private const string CursorDateTimeKey = "cursorDateTime";

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
        /// Blob containing the cursor data. Cursor data comprises of cursorDateTime
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

            if (Source == null)
            {
                throw new ArgumentNullException("Source cannot be null");
            }

            if (PrimaryDestination == null)
            {
                throw new ArgumentNullException("Primary Destination cannot be null");
            }

            if (PackageDatabase == null)
            {
                throw new ArgumentNullException("PackageDatabase cannot be null");
            }

            SourceContainer = Source.CreateCloudBlobClient().GetContainerReference(
                String.IsNullOrEmpty(SourceContainerName) ? BlobContainerNames.LegacyPackages : SourceContainerName);
            PrimaryDestinationContainer = PrimaryDestination.CreateCloudBlobClient().GetContainerReference(
                String.IsNullOrEmpty(DestinationContainerName) ? BlobContainerNames.Backups : DestinationContainerName);
            SecondaryDestinationContainer = SecondaryDestination == null ? null : SecondaryDestination.CreateCloudBlobClient().GetContainerReference(
                String.IsNullOrEmpty(DestinationContainerName) ? BlobContainerNames.Backups : DestinationContainerName);
            CursorBlob = String.IsNullOrEmpty(CursorBlob) ? DefaultCursorBlob : CursorBlob;

            Log.PreparingToArchive(Source.Credentials.AccountName, SourceContainer.Name, PrimaryDestination.Credentials.AccountName, PrimaryDestinationContainer.Name, PackageDatabase.DataSource, PackageDatabase.InitialCatalog);
            await Archive(PrimaryDestinationContainer);

            if (SecondaryDestinationContainer != null)
            {
                Log.PreparingToArchive2(SecondaryDestination.Credentials.AccountName, SecondaryDestinationContainer.Name);
                await Archive(SecondaryDestinationContainer);
            }
        }

        private async Task Archive(CloudBlobContainer destinationContainer)
        {
            var cursorJObject = await GetJObject(destinationContainer, CursorBlob);
            var cursorDateTime = cursorJObject[CursorDateTimeKey].Value<DateTime>();

            Log.CursorData(cursorDateTime.ToString(DateTimeFormatSpecifier));

            Log.GatheringPackagesToArchiveFromDB(PackageDatabase.DataSource, PackageDatabase.InitialCatalog);
            List<PackageRef> packages;
            using (var connection = await PackageDatabase.ConnectTo())
            {
                packages = (await connection.QueryAsync<PackageRef>(@"
			    SELECT pr.Id, p.NormalizedVersion AS Version, p.Hash, p.LastEdited, p.Published
			    FROM Packages p
			    INNER JOIN PackageRegistrations pr ON p.PackageRegistrationKey = pr.[Key]
			    WHERE Published > @cursorDateTime OR LastEdited > @cursorDateTime", new { cursorDateTime = cursorDateTime }))
                    .ToList();
            }
            Log.GatheredPackagesToArchiveFromDB(packages.Count, PackageDatabase.DataSource, PackageDatabase.InitialCatalog);

            var archiveSet = packages
                .AsParallel()
                .Select(r => Tuple.Create(StorageHelpers.GetPackageBlobName(r), StorageHelpers.GetPackageBackupBlobName(r)))
                .ToList();

            if (!WhatIf)
            {
                await destinationContainer.CreateIfNotExistsAsync();
            }

            if (archiveSet.Count > 0)
            {
                Log.StartingArchive(archiveSet.Count);
                await Extend(TimeSpan.FromMinutes(archiveSet.Count * 10));
                foreach (var archiveItem in archiveSet)
                {
                    await ArchivePackage(archiveItem.Item1, archiveItem.Item2, SourceContainer, destinationContainer);
                }

                var maxLastEdited = packages.Max(p => p.LastEdited);
                var maxPublished = packages.Max(p => p.Published);

                // Time is ever increasing after all, simply store the max of published and lastEdited as cursorDateTime
                var newCursorDateTime = maxLastEdited > maxPublished ? new DateTime(maxLastEdited.Value.Ticks, DateTimeKind.Utc) : new DateTime(maxPublished.Value.Ticks, DateTimeKind.Utc);
                var newCursorDateTimeString = newCursorDateTime.ToString(DateTimeFormatSpecifier);

                Log.NewCursorData(newCursorDateTimeString);
                cursorJObject[CursorDateTimeKey] = newCursorDateTimeString;
                await SetJObject(destinationContainer, CursorBlob, cursorJObject);
            }
        }

        private async Task ArchivePackage(string sourceBlobName, string destinationBlobName, CloudBlobContainer sourceContainer, CloudBlobContainer destinationContainer)
        {
            // Identify the source and destination blobs
            var sourceBlob = sourceContainer.GetBlockBlobReference(sourceBlobName);
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
            Message = "Cursor data: CursorDateTime is {0}")]
        public void CursorData(string cursorDateTime) { WriteEvent(3, cursorDateTime); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Task = Tasks.GatheringDBPackages,
            Opcode = EventOpcode.Start,
            Message = "Gathering list of packages to archive from {0}/{1}")]
        public void GatheringPackagesToArchiveFromDB(string dbServer, string dbName) { WriteEvent(4, dbServer, dbName); }

        [Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Task = Tasks.GatheringDBPackages,
            Opcode = EventOpcode.Stop,
            Message = "Gathered {0} packages to archive from {1}/{2}")]
        public void GatheredPackagesToArchiveFromDB(int gathered, string dbServer, string dbName) { WriteEvent(5, gathered, dbServer, dbName); }

        [Event(
            eventId: 6,
            Level = EventLevel.Informational,
            Task = Tasks.ArchivingPackages,
            Opcode = EventOpcode.Start,
            Message = "Starting archive of {0} packages.")]
        public void StartingArchive(int count) { WriteEvent(6, count); }

        [Event(
            eventId: 7,
            Level = EventLevel.Informational,
            Task = Tasks.ArchivingPackages,
            Opcode = EventOpcode.Stop,
            Message = "Started archive.")]
        public void StartedArchive() { WriteEvent(7); }

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

        [Event(
            eventId: 14,
            Level = EventLevel.Informational,
            Message = "NewCursor data: CursorDateTime is {0}")]
        public void NewCursorData(string cursorDateTime) { WriteEvent(14, cursorDateTime); }
    }

    public static class Tasks
    {
        public const EventTask GatheringDBPackages = (EventTask)0x1;
        public const EventTask ArchivingPackages = (EventTask)0x2;
        public const EventTask StartingPackageCopy = (EventTask)0x3;
    }
}
