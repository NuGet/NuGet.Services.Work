using System;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Globalization;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Jobs.Models;
using System.ComponentModel;

namespace NuGet.Services.Work.Jobs
{
    [Description("Maintains fragmented indexes on the specified SQL Database")]
    public class MaintainDatabaseIndexesJob : DatabaseJobHandlerBase<RebuildDatabaseIndexesEventSource>
    {
        private static readonly string CollectIndexNamesSql = @"
            SELECT objs.name AS ObjectName, idx.name AS IndexName, stats.avg_fragmentation_in_percent as Fragmentation, (CASE WHEN (stats.avg_fragmentation_in_percent > 30) THEN 1 ELSE 0 END) AS FullRebuild
            FROM sys.indexes idx
            INNER JOIN sys.objects objs ON idx.object_id = objs.object_id
            CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(), idx.[object_id], idx.index_id, 0, NULL) stats
            WHERE stats.avg_fragmentation_in_percent > 5
            ORDER BY stats.avg_fragmentation_in_percent DESC";
        private static readonly string ReorganizeScriptFormatter = @"ALTER INDEX {0} ON {1} REORGANIZE;";
        private static readonly string RebuildScriptFormatter = @"ALTER INDEX {0} ON {1} REBUILD WITH (ONLINE=ON);";
        private static readonly string RebuildOperation = "rebuild";
        private static readonly string ReorganizeOperation = "reorganization";

        public MaintainDatabaseIndexesJob(ConfigurationHub config) : base(config) { }

        protected internal override async Task<JobContinuation> Execute()
        {
            // Get the indexes to perform maintenance on
            var cstr = GetConnectionString();
            using (var connection = await cstr.ConnectTo())
            {
                // Collect indexes to be maintained
                Log.CollectingIndexStatistics(cstr.DataSource, cstr.InitialCatalog);
                var indexes = (await connection.QueryAsync<IndexStatistic>(CollectIndexNamesSql)).ToList();
                Log.CollectedIndexStatistics(cstr.DataSource, cstr.InitialCatalog, indexes.Count);

                foreach (var index in indexes)
                {
                    string operation = index.FullRebuild ? RebuildOperation : ReorganizeOperation;
                    string scriptFormatter = index.FullRebuild ? RebuildScriptFormatter : ReorganizeScriptFormatter;
                    Log.MaintainingIndex(operation, index.IndexName, index.ObjectName);
                    if (!WhatIf)
                    {
                        // Yep, SQL Injection! But this is an admin task!
                        await connection.ExecuteAsync(String.Format(scriptFormatter, index.IndexName, index.ObjectName));
                    }
                    Log.MaintainedIndex(operation, index.IndexName, index.ObjectName);
                }
            }

            return Complete();
        }

        public class IndexStatistic
        {
            public string ObjectName { get; set; }
            public string IndexName { get; set; }
            public float Fragmentation { get; set; }
            public bool FullRebuild { get; set; }
        }
    }

    [EventSource(Name="Outercurve-NuGet-Jobs-RebuildDatabaseIndexes")]
    public class RebuildDatabaseIndexesEventSource : EventSource
    {
        public static readonly RebuildDatabaseIndexesEventSource Log = new RebuildDatabaseIndexesEventSource();

        private RebuildDatabaseIndexesEventSource() { }

        [Event(
            eventId: 1,
            Task = Tasks.CollectingIndexStatistics,
            Opcode = EventOpcode.Start,
            Message = "Collecting index statistics for {0}/{1}",
            Level = EventLevel.Informational)]
        public void CollectingIndexStatistics(string server, string database) { WriteEvent(1, server, database); }

        [Event(
            eventId: 2,
            Task = Tasks.CollectingIndexStatistics,
            Opcode = EventOpcode.Stop,
            Message = "Collected index statistics for {0}/{1}. {2} indexes to be maintained",
            Level = EventLevel.Informational)]
        public void CollectedIndexStatistics(string server, string database, int count) { WriteEvent(2, server, database, count); }

        [Event(
            eventId: 3,
            Task = Tasks.MaintainingIndex,
            Opcode = EventOpcode.Start,
            Message = "Starting {0} of index {1} on {2}",
            Level = EventLevel.Informational)]
        public void MaintainingIndex(string operation, string index, string obj) { WriteEvent(3, operation, index, obj); }

        [Event(
            eventId: 4,
            Task = Tasks.MaintainingIndex,
            Opcode = EventOpcode.Start,
            Message = "Completed {0} of index {1} on {2}",
            Level = EventLevel.Informational)]
        public void MaintainedIndex(string operation, string index, string obj) { WriteEvent(4, operation, index, obj); }

        public static class Tasks
        {
            public const EventTask CollectingIndexStatistics = (EventTask)0x1;
            public const EventTask MaintainingIndex = (EventTask)0x2;
        }
    }
}
