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
using System.Data;

namespace NuGet.Services.Work.Jobs
{
    [Description("Maintains fragmented indexes on the specified SQL Database")]
    public class MaintainDatabaseIndexesJob : DatabaseJobHandlerBase<RebuildDatabaseIndexesEventSource>
    {
        private static readonly string CollectIndexNamesSql = @"
            SELECT objs.name AS ObjectName, idx.name AS IndexName, stats.avg_fragmentation_in_percent as Fragmentation
            FROM sys.indexes idx
            INNER JOIN sys.objects objs ON idx.object_id = objs.object_id
            CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(), idx.[object_id], idx.index_id, 0, NULL) stats
            WHERE stats.avg_fragmentation_in_percent > @threshold
            ORDER BY stats.avg_fragmentation_in_percent DESC";
        private static readonly string RebuildScriptFormatter = @"ALTER INDEX {0} ON {1} REBUILD WITH (ONLINE=OFF);"; // Can't do online rebuild of tables with LOB columns like nvarchar(MAX)
        
        public static readonly float DefaultFragmentationThreshold = 20.0f;

        public float? FragmentationThreshold { get; set; }

        public MaintainDatabaseIndexesJob(ConfigurationHub config) : base(config) { }

        protected internal override async Task<JobContinuation> Execute()
        {
            // Get default values
            var threshold = FragmentationThreshold ?? DefaultFragmentationThreshold;

            // Get the indexes to perform maintenance on
            var cstr = GetConnectionString();
            using (var connection = await cstr.ConnectTo())
            {
                // Collect indexes to be maintained
                Log.CollectingIndexStatistics(cstr.DataSource, cstr.InitialCatalog, threshold);
                var indexes = (await connection.QueryAsync<IndexStatistic>(CollectIndexNamesSql, new { threshold })).ToList();
                Log.CollectedIndexStatistics(cstr.DataSource, cstr.InitialCatalog, indexes.Count);

                foreach (var index in indexes)
                {
                    Log.MaintainingIndex(index.IndexName, index.ObjectName, index.Fragmentation);
                    if (!WhatIf)
                    {
                        // Yep, SQL Injection! But this is an admin task!
                        await connection.QueryAsync<int>(
                            String.Format(RebuildScriptFormatter, index.IndexName, index.ObjectName),
                            param:null, 
                            transaction:null, 
                            commandTimeout:0, 
                            commandType:CommandType.Text);
                    }
                    Log.MaintainedIndex(index.IndexName, index.ObjectName);
                }

                // Recheck index status
                Log.CollectingIndexStatistics(cstr.DataSource, cstr.InitialCatalog, 0.0f);
                indexes = (await connection.QueryAsync<IndexStatistic>(CollectIndexNamesSql, new { threshold = 0.0f })).ToList();
                Log.CollectedIndexStatistics(cstr.DataSource, cstr.InitialCatalog, indexes.Count);

                foreach (var index in indexes)
                {
                    Log.IndexStatus(index.ObjectName, index.IndexName, index.Fragmentation);
                }

                Log.UpdatingStats();
                if (!WhatIf)
                {
                    await connection.ExecuteAsync("EXEC sp_updatestats");
                }
                Log.UpdatedStats();
            }

            return Complete();
        }

        public class IndexStatistic
        {
            public string ObjectName { get; set; }
            public string IndexName { get; set; }
            public float Fragmentation { get; set; }
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
            Message = "Collecting index statistics for indexes over {2:000.0}% fragmentation on {0}/{1}",
            Level = EventLevel.Informational)]
        public void CollectingIndexStatistics(string server, string database, float threshold) { WriteEvent(1, server, database, threshold); }

        [Event(
            eventId: 2,
            Task = Tasks.CollectingIndexStatistics,
            Opcode = EventOpcode.Stop,
            Message = "Collected index statistics for {2} indexes on {0}/{1}.",
            Level = EventLevel.Informational)]
        public void CollectedIndexStatistics(string server, string database, int count) { WriteEvent(2, server, database, count); }

        [Event(
            eventId: 3,
            Task = Tasks.MaintainingIndex,
            Opcode = EventOpcode.Start,
            Message = "Starting rebuild of index {0} on {1} ({2:000.00}% Fragmented)",
            Level = EventLevel.Informational)]
        public void MaintainingIndex(string index, string obj, float fragmentation) { WriteEvent(3, index, obj, fragmentation); }

        [Event(
            eventId: 4,
            Task = Tasks.MaintainingIndex,
            Opcode = EventOpcode.Start,
            Message = "Completed rebuild of index {0} on {1}",
            Level = EventLevel.Informational)]
        public void MaintainedIndex(string index, string obj) { WriteEvent(4, index, obj); }

        [Event(
            eventId: 5,
            Message = "[Fragmentation {2:000.0}%]: Index {0}/{1}",
            Level = EventLevel.Informational)]
        public void IndexStatus(string obj, string index, float fragmentation) { WriteEvent(5, obj, index, fragmentation); }

        [Event(
            eventId: 6,
            Task = Tasks.UpdatingStats,
            Opcode = EventOpcode.Start,
            Message = "Updating Query Statistics",
            Level = EventLevel.Informational)]
        public void UpdatingStats() { WriteEvent(6); }

        [Event(
            eventId: 7,
            Task = Tasks.UpdatingStats,
            Opcode = EventOpcode.Stop,
            Message = "Updated Query Statistics",
            Level = EventLevel.Informational)]
        public void UpdatedStats() { WriteEvent(7); }

        public static class Tasks
        {
            public const EventTask CollectingIndexStatistics = (EventTask)0x1;
            public const EventTask MaintainingIndex = (EventTask)0x2;
            public const EventTask UpdatingStats = (EventTask)0x3;
        }
    }
}
