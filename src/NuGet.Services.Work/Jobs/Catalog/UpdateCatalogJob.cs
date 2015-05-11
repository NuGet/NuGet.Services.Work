// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NuGet.Services.Configuration;
using NuGet.Services.Metadata.Catalog.Collecting;
using NuGet.Services.Metadata.Catalog.Maintenance;
using NuGet.Services.Metadata.Catalog.Persistence;
using NuGet.Services.Work.Monitoring;

namespace NuGet.Services.Work.Jobs.Catalog
{
    public class UpdateCatalogJob : JobHandler<UpdateCatalogEventSource>
    {
        public static readonly int DefaultChecksumCollectorBatchSize = 2000;
        public static readonly int DefaultCatalogPageSize = 1000;

        private readonly ConfigurationHub Config;

        public SqlConnectionStringBuilder SourceDatabase { get; set; }
        public CloudStorageAccount CatalogStorage { get; set; }
        public string CatalogPath { get; set; }
        public int? ChecksumCollectorBatchSize { get; set; }
        public int? CatalogPageSize { get; set; }

        public UpdateCatalogJob(ConfigurationHub config)
        {
            AddEventSource(CatalogUpdaterEventSource.Log);
            AddEventSource(ChecksumCollectorEventSource.Log, EventLevel.Informational);

            Config = config;
        }

        protected internal override async Task Execute()
        {
            await Extend(TimeSpan.FromMinutes(10));

            var collectorBatchSize = ChecksumCollectorBatchSize ?? DefaultChecksumCollectorBatchSize;
            var catalogPageSize = CatalogPageSize ?? DefaultCatalogPageSize;

            // Load Default values
            SourceDatabase = Config.Sql.Legacy;
            CatalogStorage = Config.Storage.Primary;

            // Process:
            //  1. Load existing checksums file, if present
            //  2. Collect new checksums
            //  3. Process updates
            //  4. Collect new checksums
            //  5. Save existing checksums file

            // Set up helpers/contexts/etc.
            var catalogDirectory = StorageHelpers.GetBlobDirectory(CatalogStorage, CatalogPath);
            var checksums = new AzureStorageChecksumRecords(catalogDirectory.GetBlockBlobReference(AzureStorageChecksumRecords.DefaultChecksumFileName));
            var checksumCollector = new ChecksumCollector(collectorBatchSize, checksums);
            var http = CreateHttpClient();
            var indexBlob = catalogDirectory.GetBlockBlobReference("index.json");
            var storage = new AzureStorage(catalogDirectory);

            // Disposing of CatalogUpdater will dispose the HTTP client, 
            // so don't move this 'using' further in or we might dispose the HTTP client before we actually finish with it!
            using (var updater = new CatalogUpdater(new CatalogWriter(storage, new CatalogContext(), catalogPageSize), checksums, http))
            {
                // 1. Load Checkums
                Log.LoadingChecksums(checksums.Uri.ToString());
                await checksums.Load();
                Log.LoadedChecksums(checksums.Data.Count);

                await ExtendIfNeeded(TimeSpan.FromMinutes(10));

                // 2. Collect new checksums
                Log.CollectingChecksums(catalogDirectory.Uri.ToString());
                await checksumCollector.Run(http, indexBlob.Uri, checksums.Cursor);
                Log.CollectedChecksums(checksums.Data.Count);

                await ExtendIfNeeded(TimeSpan.FromMinutes(10));

                // 3. Process updates
                Log.UpdatingCatalog();
                await updater.Update(SourceDatabase.ConnectionString, indexBlob.Uri);
                Log.UpdatedCatalog();

                await ExtendIfNeeded(TimeSpan.FromMinutes(10));

                // 4. Collect new checksums
                Log.CollectingChecksums(catalogDirectory.Uri.ToString());
                await checksumCollector.Run(http, indexBlob.Uri, checksums.Cursor);
                Log.CollectedChecksums(checksums.Data.Count);

                await ExtendIfNeeded(TimeSpan.FromMinutes(10));

                // 5. Save existing checksums file
                Log.SavingChecksums(checksums.Uri.ToString());
                await checksums.Save();
                Log.SavedChecksums();

                await ExtendIfNeeded(TimeSpan.FromMinutes(10));
            }

            await this.Enqueue(this.Invocation.Job, this.Invocation.Payload, TimeSpan.FromSeconds(3), this.Invocation.JobInstanceName);
        }

        private CollectorHttpClient CreateHttpClient()
        {
            var tracer = new TracingHttpHandler(new HttpClientHandler());
            tracer.OnSend += request =>
            {
                Log.SendingHttpRequest(request.Method.ToString(), request.RequestUri.ToString());
            };
            tracer.OnException += (request, exception) =>
            {
                Log.HttpException(request.RequestUri.ToString(), exception.ToString());
            };
            tracer.OnReceive += (request, response) =>
            {
                Log.ReceivedHttpResponse((int)response.StatusCode, request.RequestUri.ToString());
            };
            return new CollectorHttpClient(tracer);
        }
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-UpdateCatalog")]
    public class UpdateCatalogEventSource : EventSource
    {
        public static readonly UpdateCatalogEventSource Log = new UpdateCatalogEventSource();
        private UpdateCatalogEventSource()
        { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.LoadingChecksums,
            Message = "Loading checksums from {0}")]
        public void LoadingChecksums(string uri) { WriteEvent(1, uri); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.LoadingChecksums,
            Message = "Loaded {0} checksums.")]
        public void LoadedChecksums(int count) { WriteEvent(2, count); }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.HttpRequest,
            Message = "{0} {1}")]
        public void SendingHttpRequest(string method, string uri) { WriteEvent(3, method, uri); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.HttpRequest,
            Message = "{0} {1}")]
        public void ReceivedHttpResponse(int statusCode, string uri) { WriteEvent(4, statusCode, uri); }

        [Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.HttpRequest,
            Message = "{0} {1}")]
        public void HttpException(string uri, string exception) { WriteEvent(5, uri, exception); }

        [Event(
            eventId: 6,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.CollectingChecksums,
            Message = "Collecting checksums from catalog at {0}")]
        public void CollectingChecksums(string uri) { WriteEvent(6, uri); }

        [Event(
            eventId: 7,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.CollectingChecksums,
            Message = "Collected new checksums. Total now {0}")]
        public void CollectedChecksums(int count) { WriteEvent(7, count); }

        [Event(
            eventId: 8,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.UpdatingCatalog,
            Message = "Running Catalog Updater")]
        public void UpdatingCatalog() { WriteEvent(8); }

        [Event(
            eventId: 9,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.UpdatingCatalog,
            Message = "Catalog Updater Completed")]
        public void UpdatedCatalog() { WriteEvent(9); }

        [Event(
            eventId: 10,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.SavingChecksums,
            Message = "Saving checksums to {0}")]
        public void SavingChecksums(string uri) { WriteEvent(10, uri); }

        [Event(
            eventId: 11,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.SavingChecksums,
            Message = "Saved checksums.")]
        public void SavedChecksums() { WriteEvent(11); }

        public static class Tasks
        {
            public const EventTask LoadingChecksums = (EventTask)0x1;
            public const EventTask HttpRequest = (EventTask)0x2;
            public const EventTask CollectingChecksums = (EventTask)0x2;
            public const EventTask UpdatingCatalog = (EventTask)0x3;
            public const EventTask SavingChecksums = (EventTask)0x4;
        }
    }
}
