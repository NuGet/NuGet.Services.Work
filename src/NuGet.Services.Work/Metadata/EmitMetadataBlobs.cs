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

namespace NuGet.Services.Work.Jobs
{
    public class EmitResolverBlobsJob : JobHandler<EmitResolverBlobsEventSource>
    {
        public EmitResolverBlobsJob(ConfigurationHub config, StorageHub storage)
        {
            Storage = storage;
            Config = config;
        }

        ConfigurationHub Config { get; set; }
        StorageHub Storage { get; set; }

        public string CatalogUri { get; set; }
        //public string ConnectionString { get; set; }
        public string Container { get; set; }
        public string BaseAddress { get; set; }

        protected internal override async Task Execute()
        {
            NuGet.Services.Metadata.Catalog.Persistence.Storage storage = new AzureStorage
            {
                ConnectionString = Storage.Legacy.ConnectionString,
                Container = Container,
                BaseAddress = BaseAddress
            };

            Uri cursorUri = new Uri(BaseAddress + Container + "/meta/cursor.json");

            StorageContent content = await storage.Load(cursorUri);
            DateTime since;

            if (content == null)
            {
                since = DateTime.MinValue.ToUniversalTime();
            }
            else
            {
                JToken cursorDoc = JsonLD.Util.JSONUtils.FromInputStream(content.GetContentStream());
                since = cursorDoc["http://nuget.org/collector/resolver#cursor"]["@value"].ToObject<DateTime>();
            }

            Uri requestUri = new Uri(CatalogUri);

            ResolverCollector collector = new ResolverCollector(storage, 200) { Logger = EmitResolverBlobsEventSource.Log };
            since = (DateTime)await collector.Run(requestUri, since);

            await storage.Save(cursorUri, new StringStorageContent(new JObject { { "http://nuget.org/collector/resolver#cursor", new JObject { { "@value", since.ToString("O") }, { "@type", "http://www.w3.org/2001/XMLSchema#dateTime" } } }, { "http://nuget.org/collector/resolver#source", CatalogUri } }.ToString()));

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
        Message = "Emitted metadata blob '{0}'"
        )
        ]
        public void EmitBlob(string blobname) { WriteEvent(1, blobname); }
    }
}
