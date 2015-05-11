// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using NuGet.Services.Metadata.Catalog;
using NuGet.Services.Metadata.Catalog.Collecting;
using NuGet.Services.Metadata.Catalog.Persistence;
using NuGet.Services.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json.Linq;
using Microsoft.WindowsAzure.Storage;
using NuGet.Services.Work.Monitoring;
using System.Net.Http;

namespace NuGet.Services.Work.Jobs
{
    public class EmitPackageRegistrationBlobsJob : JobHandler<EmitPackageRegistrationsBlobsEventSource>
    {
        private readonly ConfigurationHub Config;

        public EmitPackageRegistrationBlobsJob(ConfigurationHub config)
        {
            Config = config;

            AddEventSource(ResolverCollectorEventSource.Log);
        }

        public CloudStorageAccount TargetStorageAccount { get; set; }
        public string TargetPath { get; set; }
        public string TargetBaseAddress { get; set; }
        public string TargetLocalDirectory { get; set; }
        public string CatalogIndexUrl { get; set; }
        public string CdnBaseAddress { get; set; }
        public string GalleryBaseAddress { get; set; }

        protected internal override async Task Execute()
        {
            await Extend(TimeSpan.FromMinutes(10));

            // Set defaults
            TargetStorageAccount = TargetStorageAccount ?? Config.Storage.Primary;

            // Check required payload
            ArgCheck.Require(TargetBaseAddress, "TargetBaseAddress");
            ArgCheck.Require(CatalogIndexUrl, "CatalogIndexUrl");
            ArgCheck.Require(CdnBaseAddress, "CdnBaseAddress");
            ArgCheck.Require(GalleryBaseAddress, "GalleryBaseAddress");

            // Clean input data
            if (!TargetBaseAddress.EndsWith("/"))
            {
                TargetBaseAddress += "/";
            }
            var resolverBaseUri = new Uri(TargetBaseAddress);
            CdnBaseAddress = CdnBaseAddress.TrimEnd('/');
            GalleryBaseAddress = GalleryBaseAddress.TrimEnd('/');
            
            // Load Storage
            NuGet.Services.Metadata.Catalog.Persistence.Storage storage;
            string storageDesc;
            if (String.IsNullOrEmpty(TargetLocalDirectory))
            {
                ArgCheck.Require(TargetStorageAccount, "ResolverStorage");
                ArgCheck.Require(TargetPath, "ResolverPath");
                var dir = StorageHelpers.GetBlobDirectory(TargetStorageAccount, TargetPath);
                storage = new AzureStorage(dir, resolverBaseUri);
                storageDesc = dir.Uri.ToString();
            }
            else
            {
                storage = new FileStorage(TargetBaseAddress, TargetLocalDirectory);
                storageDesc = TargetLocalDirectory;
            }


            Uri cursorUri = new Uri(resolverBaseUri, "meta/cursor.json");

            Log.LoadingCursor(cursorUri.ToString());
            StorageContent content = await storage.Load(cursorUri);
            CollectorCursor lastCursor;

            if (content == null)
            {
                lastCursor = CollectorCursor.None;
            }
            else
            {
                JToken cursorDoc = JsonLD.Util.JSONUtils.FromInputStream(content.GetContentStream());
                lastCursor = (CollectorCursor)(cursorDoc["http://schema.nuget.org/collectors/resolver#cursor"].Value<DateTime>("@value"));
            }
            Log.LoadedCursor(lastCursor.Value);

            ResolverCollector collector = new ResolverCollector(storage, 200)
            {
                ContentBaseAddress = CdnBaseAddress,
                GalleryBaseAddress = GalleryBaseAddress
            };
            
            collector.ProcessedCommit += cursor =>
            {
                ExtendIfNeeded(TimeSpan.FromMinutes(10)).Wait();
                
                if (!Equals(cursor, lastCursor))
                {
                    StoreCursor(storage, cursorUri, cursor).Wait();
                    lastCursor = cursor;
                }
            };

            Log.EmittingResolverBlobs(
                CatalogIndexUrl.ToString(),
                storageDesc,
                CdnBaseAddress,
                GalleryBaseAddress);
            lastCursor = (DateTime)await collector.Run(
                new Uri(CatalogIndexUrl), 
                lastCursor);
            Log.EmittedResolverBlobs();

            await this.Enqueue(this.Invocation.Job, this.Invocation.Payload, TimeSpan.FromSeconds(3), this.Invocation.JobInstanceName);
        }

        private async Task StoreCursor(NuGet.Services.Metadata.Catalog.Persistence.Storage storage, Uri cursorUri, CollectorCursor value)
        {
            if (!Equals(value, CollectorCursor.None))
            {
                Log.StoringCursor(value.Value);
                var cursorContent = new JObject { 
                { "http://schema.nuget.org/collectors/resolver#cursor", new JObject { 
                    { "@value", value.Value }, 
                    { "@type", "http://www.w3.org/2001/XMLSchema#dateTime" } } }, 
                { "http://schema.nuget.org/collectors/resolver#source", CatalogIndexUrl } }.ToString();
                await storage.Save(cursorUri, new StringStorageContent(
                    cursorContent,
                    contentType: "application/json",
                    cacheControl: "no-store"));
                Log.StoredCursor();
            }
        }
    }

    public class EmitPackageRegistrationsBlobsEventSource : EventSource
    {
        public static readonly EmitPackageRegistrationsBlobsEventSource Log = new EmitPackageRegistrationsBlobsEventSource();

        private EmitPackageRegistrationsBlobsEventSource() { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Emitted metadata blob '{0}'")]
        public void EmitBlob(string blobname) { WriteEvent(1, blobname); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.EmitResolverBlobs,
            Message = "Emitting Resolver Blobs to '{1}' using catalog at '{0}', cdn at '{2}', gallery at '{3}'")]
        public void EmittingResolverBlobs(string catalog, string destination, string cdnBase, string galleryBase) { WriteEvent(2, catalog, destination, cdnBase, galleryBase); }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.EmitResolverBlobs,
            Message = "Emitted Resolver Blobs.")]
        public void EmittedResolverBlobs() { WriteEvent(3); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.LoadingCursor,
            Message = "Loaded cursor: {0}")]
        public void LoadedCursor(string cursorValue) { WriteEvent(4, cursorValue); }

        [Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.StoringCursor,
            Message = "Storing next cursor: {0}")]
        public void StoringCursor(string cursorValue) { WriteEvent(5, cursorValue); }

        [Event(
            eventId: 6,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.LoadingCursor,
            Message = "Loading cursor from {0}")]
        public void LoadingCursor(string cursorUri) { WriteEvent(6, cursorUri); }

        [Event(
            eventId: 7,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.StoringCursor,
            Message = "Stored cursor.")]
        public void StoredCursor() { WriteEvent(7); }

        [Event(
            eventId: 8,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.HttpRequest,
            Message = "{0} {1}")]
        public void SendingHttpRequest(string method, string uri) { WriteEvent(8, method, uri); }

        [Event(
            eventId: 9,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.HttpRequest,
            Message = "{0} {1}")]
        public void ReceivedHttpResponse(int statusCode, string uri) { WriteEvent(9, statusCode, uri); }

        [Event(
            eventId: 10,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.HttpRequest,
            Message = "{0} {1}")]
        public void HttpException(string uri, string exception) { WriteEvent(10, uri, exception); }

        public static class Tasks
        {
            public const EventTask EmitResolverBlobs = (EventTask)0x1;
            public const EventTask LoadingCursor = (EventTask)0x2;
            public const EventTask StoringCursor = (EventTask)0x3;
            public const EventTask HttpRequest = (EventTask)0x4;
        }
    }
}
