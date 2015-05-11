// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
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

        protected internal override async Task Execute()
        {
            // This job can take a long time and is run manually. Make the job timeout long
            await Extend(TimeSpan.FromHours(12));

            // Run the task
            Log.BeginningIndex(
                PackageDatabase.DataSource + "/" + PackageDatabase.InitialCatalog,
                String.IsNullOrEmpty(LocalIndexFolder) ?
                    (StorageAccount.Credentials.AccountName + "/" + IndexContainerName) :
                    LocalIndexFolder);
            FullBuildTask task = new FullBuildTask()
            {
                SqlConnectionString = PackageDatabase.ConnectionString,
                StorageAccount = StorageAccount,
                Container = String.IsNullOrEmpty(LocalIndexFolder) ?
                    IndexContainerName :
                    null,
                DataContainer = DataContainerName,
                Folder = LocalIndexFolder,
                Log = new EventSourceWriter(Log.IndexingTrace),
            };
            task.Execute();
            Log.FinishedIndex();
        }
    }

    [EventSource(Name="Outercurve-NuGet-Jobs-RebuildSearchIndex")]
    public class RebuildSearchIndexEventSource : EventSource
    {
        public static readonly RebuildSearchIndexEventSource Log = new RebuildSearchIndexEventSource();
        private RebuildSearchIndexEventSource() { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Indexing Trace: {0}")]
        public void IndexingTrace(string message) { WriteEvent(1, message); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Message = "Beginning Index Rebuild from {0} to {1}",
            Task = Tasks.Indexing,
            Opcode = EventOpcode.Start)]
        public void BeginningIndex(string source, string destination) { WriteEvent(2, source, destination); }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Message = "Finished Index Rebuild",
            Task = Tasks.Indexing,
            Opcode = EventOpcode.Stop)]
        public void FinishedIndex() { WriteEvent(3); }

        public static class Tasks
        {
            public const EventTask Indexing = (EventTask)0x1;
        }
    }
}
