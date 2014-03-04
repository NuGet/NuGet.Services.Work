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
    public class RebuildSearchIndexJob : SearchIndexJobHandlerBase<RebuildSearchIndexEventSource>
    {
        public RebuildSearchIndexJob(ConfigurationHub config) : base(config) { }

        protected internal override Task Execute()
        {
            // Run the task
            FullBuildTask task = new FullBuildTask()
            {
                SqlConnectionString = PackageDatabase.ConnectionString,
                StorageAccount = StorageAccount,
                Container = StorageContainerName ?? "ng-search",
                Log = new EventSourceWriter(Log.IndexingTrace)
            };
            task.Execute();

            return Task.FromResult(0);
        }
    }

    [EventSource(Name="Outercurve-NuGet-Jobs-RebuildSearchIndex")]
    public class RebuildSearchIndexEventSource : EventSource
    {
        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Indexing Trace: {0}")]
        public void IndexingTrace(string message) { WriteEvent(1, message); }
    }
}
