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
using NuGet.Services.Storage;
using Microsoft.WindowsAzure.Storage;

namespace NuGet.Services.Work.Jobs
{
    public class EmitPackageRegistrationBlobsJob : JobHandler<EmitResolverBlobsEventSource>
    {
        private readonly ConfigurationHub Config;

        public EmitPackageRegistrationBlobsJob(ConfigurationHub config)
        {
            Config = config;
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
            // Disable job re-run logic
            await Extend(TimeSpan.FromDays(365));

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
            DateTime since;

            if (content == null)
            {
                since = DateTime.MinValue.ToUniversalTime();
            }
            else
            {
                JToken cursorDoc = JsonLD.Util.JSONUtils.FromInputStream(content.GetContentStream());
                since = cursorDoc["http://schema.nuget.org/collectors/resolver#cursor"]["@value"].ToObject<DateTime>();
            }
            Log.LoadedCursor(since.ToString("O"));

            ResolverCollector collector = new ResolverCollector(storage, 200)
            {
                Logger = EmitResolverBlobsEventSource.Log,
                CdnBaseAddress = CdnBaseAddress,
                GalleryBaseAddress = GalleryBaseAddress
            };
            Log.EmittingResolverBlobs(
                CatalogIndexUrl.ToString(),
                storageDesc,
                CdnBaseAddress,
                GalleryBaseAddress);
            since = (DateTime)await collector.Run(new Uri(CatalogIndexUrl), since);
            Log.EmittedResolverBlobs();

            Log.StoringCursor(since.ToString("O"));
            var cursorContent = new JObject { 
                { "http://schema.nuget.org/collectors/resolver#cursor", new JObject { 
                    { "@value", since.ToString() }, 
                    { "@type", "http://www.w3.org/2001/XMLSchema#dateTime" } } }, 
                { "http://schema.nuget.org/collectors/resolver#source", CatalogIndexUrl } }.ToString();
            await storage.Save(cursorUri, new StringStorageContent(
                cursorContent,
                contentType: "application/json",
                cacheControl: "no-store"));
            Log.StoredCursor();

            await this.Enqueue(this.Invocation.Job, this.Invocation.Payload, TimeSpan.FromSeconds(3));
        }
    }

    public class EmitResolverBlobsEventSource : EventSource, ICollectorLogger
    {
        public static readonly EmitResolverBlobsEventSource Log = new EmitResolverBlobsEventSource();

        private EmitResolverBlobsEventSource() { }

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

        public static class Tasks
        {
            public const EventTask EmitResolverBlobs = (EventTask)0x1;
            public const EventTask LoadingCursor = (EventTask)0x2;
            public const EventTask StoringCursor = (EventTask)0x3;
        }
    }
}
