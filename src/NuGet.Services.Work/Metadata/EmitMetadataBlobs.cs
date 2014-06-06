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

namespace NuGet.Services.Work.Jobs
{
    public class EmitResolverBlobsJob : JobHandler<EmitResolverBlobsEventSource>
    {
        public EmitResolverBlobsJob(ConfigurationHub config) { }

        public string CatalogUri { get; set; }
        public string AccountKey { get; set; }
        public string AccountName { get; set; }
        public string ConnectionString { get; set; }
        public string Container { get; set; }
        public string BaseAddress { get; set; }

        protected internal override async Task Execute()
        {
            NuGet.Services.Metadata.Catalog.Persistence.Storage storage = new AzureStorage
            {
                AccountKey = AccountKey,
                AccountName = AccountName,
                ConnectionString = ConnectionString,
                Container = Container,
                BaseAddress = BaseAddress
            };

            Uri cursorUri = new Uri(BaseAddress + Container + "/meta/cursor.json");

            StorageContent content = await storage.Load(cursorUri);
            DateTime since;

            if (content == null)
            {
                since = DateTime.MinValue;
            }
            else
            {
                JToken cursorDoc = JsonLD.Util.JSONUtils.FromInputStream(content.GetContentStream());
                since = DateTime.Parse((string)cursorDoc["cursor"]);
            }

            Uri requestUri = new Uri(CatalogUri);

            ResolverCollector collector = new ResolverCollector(storage, 200) { Logger = EmitResolverBlobsEventSource.Log };
            since = (DateTime)await collector.Run(requestUri, since);

            await storage.Save(cursorUri, new StringStorageContent(new JObject { { "cursor", since.ToString() } }.ToString()));

            InvocationState state = await this.Enqueue(this.Invocation.Job, this.Invocation.Payload, TimeSpan.FromSeconds(3));
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
