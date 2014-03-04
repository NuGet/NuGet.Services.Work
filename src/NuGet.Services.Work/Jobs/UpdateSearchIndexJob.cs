using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NuGet.Indexing;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Jobs.Bases;
using NuGet.Services.Work.Monitoring;

namespace NuGet.Services.Work.Jobs
{
    public class UpdateSearchIndexJob : SearchIndexJobHandlerBase<UpdateSearchIndexEventSource>
    {
        public UpdateSearchIndexJob(ConfigurationHub config) : base(config) { }

        protected internal override Task Execute()
        {
            // Run the task
            UpdateIndexTask task = new UpdateIndexTask()
            {                
                SqlConnectionString = PackageDatabase.ConnectionString,
                StorageAccount = StorageAccount,
                Container = StorageContainerName ?? "ng-search",
                Log = new EventSourceWriter(Log.IndexingTrace),
            };
            task.Execute();

            return Task.FromResult(0);
        }
    }

    [EventSource(Name="Outercurve-NuGet-Jobs-UpdateSearchIndex")]
    public class UpdateSearchIndexEventSource : EventSource
    {
        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Indexing Trace: {0}")]
        public void IndexingTrace(string message) { WriteEvent(1, message); }
    }
}
