using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using NuGet.Services.Configuration;

namespace NuGet.Services.Work.Jobs
{
    [Description("Purges package statistics from the primary database which have been synced to the warehouse")]
    public class PurgePackageStatisticsJob : JobHandler<PurgePackageStatisticsEventSource>
    {
        public static readonly int DefaultBatchSize = 50000;

        /// <summary>
        /// Gets or sets a connection string to the database containing package data.
        /// </summary>
        public SqlConnectionStringBuilder Source { get; set; }

        /// <summary>
        /// Gets or sets a connection string to the database containing warehouse data.
        /// </summary>
        public SqlConnectionStringBuilder Destination { get; set; }

        public int? BatchSize { get; set; }

        protected ConfigurationHub Config { get; private set; }

        public PurgePackageStatisticsJob(ConfigurationHub config)
        {
            Config = config;
        }

        protected internal override async Task Execute()
        {
            // Load defaults
            Source = Source ?? Config.Sql.Legacy;
            Destination = Destination ?? Config.Sql.Warehouse;
            BatchSize = BatchSize ?? DefaultBatchSize;

            Log.GettingLastReplicatedKey(Destination.DataSource, Destination.InitialCatalog);
            int originalKey = await ReplicatePackageStatisticsJob.GetLastOriginalKey(Destination);
            Log.GotLastReplicatedKey(Destination.DataSource, Destination.InitialCatalog, originalKey);

            Log.PurgingStatistics(Source.DataSource, Source.InitialCatalog, Destination.DataSource, Destination.InitialCatalog);
            int purged = await DeletePackageStatistics(originalKey);
            Log.PurgedStatistics(Source.DataSource, Source.InitialCatalog, Destination.DataSource, Destination.InitialCatalog, purged);
        }

        private async Task<int> DeletePackageStatistics(int warehouseHighWatermark)
        {
            var windowEnd = DateTime.UtcNow.AddDays(-7);

            using (var connection = await Source.ConnectTo())
            {
                int total = 0;
                StatisticsPurgeResult result;
                do
                {
                    Log.PurgingStatisticsBatch(Source.DataSource, Source.InitialCatalog, Destination.DataSource, Destination.InitialCatalog, BatchSize.Value);
                    if (WhatIf)
                    {
                        result = null;
                    }
                    else
                    {
                        result = (await connection.QueryAsync<StatisticsPurgeResult>(@"
                            DECLARE @t AS TABLE(
		                        [Key] int PRIMARY KEY
	                        )

                            DELETE TOP(@BatchSize) [PackageStatistics]
                                OUTPUT deleted.[Key] INTO @t
                            WHERE [Key] <= @OriginalKey
                            AND [Key] <= (SELECT DownloadStatsLastAggregatedId FROM GallerySettings)
                            AND [TimeStamp] < @windowEnd

                            SELECT MIN([Key]) AS OldestKeyDeleted, MAX([Key]) AS NewestKeyDeleted, COUNT(1) AS DeletedCount FROM @t
                        ", new
                             {
                                 OriginalKey = warehouseHighWatermark,
                                 BatchSize = BatchSize.Value,
                                 windowEnd
                             })).SingleOrDefault();
                    }

                    await ExtendIfLessThan(TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(5));

                    Log.PurgedStatisticsBatch(Source.DataSource, Source.InitialCatalog, Destination.DataSource, Destination.InitialCatalog, result.DeletedCount, result.OldestKeyDeleted, result.NewestKeyDeleted);
                    total += result.DeletedCount;
                }
                while (result != null && result.DeletedCount > 0);
                return total;
            }
        }

        public class StatisticsPurgeResult
        {
            public int OldestKeyDeleted { get; set; }
            public int NewestKeyDeleted { get; set; }
            public int DeletedCount { get; set; }
        }
    }

    [EventSource(Name="Outercurve-NuGet-Jobs-PurgePackageStatistics")]
    public class PurgePackageStatisticsEventSource : EventSource
    {
        public static readonly PurgePackageStatisticsEventSource Log = new PurgePackageStatisticsEventSource();
        private PurgePackageStatisticsEventSource() { }

        [Event(
            eventId: 1,
            Task = Tasks.PurgingStatistics,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Purging statistics in {0}/{1} that have been synced to {2}/{3}")]
        public void PurgingStatistics(string sourceServer, string sourceDatabase, string destServer, string destDatabase) { WriteEvent(1, sourceServer, sourceDatabase, destServer, destDatabase); }

        [Event(
            eventId: 2,
            Task = Tasks.PurgingStatistics,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Purged {4} statistics in {0}/{1} that have been synced to {2}/{3}")]
        public void PurgedStatistics(string sourceServer, string sourceDatabase, string destServer, string destDatabase, int count) { WriteEvent(2, sourceServer, sourceDatabase, destServer, destDatabase, count); }

        [Event(
            eventId: 3,
            Task = Tasks.PurgingStatisticsBatch,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Purging batch of {4} statistics in {0}/{1} that have been synced to {2}/{3}")]
        public void PurgingStatisticsBatch(string sourceServer, string sourceDatabase, string destServer, string destDatabase, int batchSize) { WriteEvent(3, sourceServer, sourceDatabase, destServer, destDatabase, batchSize); }

        [Event(
            eventId: 4,
            Task = Tasks.PurgingStatisticsBatch,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Purged batch of {4} statistics (from {5} to {6}) in {0}/{1} that have been synced to {2}/{3}")]
        public void PurgedStatisticsBatch(string sourceServer, string sourceDatabase, string destServer, string destDatabase, int count, int oldest, int newest) { WriteEvent(4, sourceServer, sourceDatabase, destServer, destDatabase, count, oldest, newest); }

        [Event(
            eventId: 5,
            Task = Tasks.GettingLastReplicatedKey,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Getting last replicated key from {0}/{1}")]
        public void GettingLastReplicatedKey(string server, string database) { WriteEvent(5, server, database); }

        [Event(
            eventId: 6,
            Task = Tasks.GettingLastReplicatedKey,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Last replicated key from {0}/{1} is {2}")]
        public void GotLastReplicatedKey(string server, string database, int key) { WriteEvent(6, server, database, key); }

        public static class Tasks
        {
            public const EventTask PurgingStatistics = (EventTask)0x1;
            public const EventTask PurgingStatisticsBatch = (EventTask)0x2;
            public const EventTask GettingLastReplicatedKey = (EventTask)0x3;
        }
    }
}
