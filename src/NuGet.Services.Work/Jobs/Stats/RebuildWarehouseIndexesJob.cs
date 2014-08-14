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
    [Description("Rebuilds the SQL Indexes in the Warehouse")]
    public class RebuildWarehouseIndexesJob : JobHandler<RebuildWarehouseIndexesJobEventSource>
    {
        /// <summary>
        /// Gets or sets a connection string to the database containing package data.
        /// </summary>
        public SqlConnectionStringBuilder WarehouseConnection { get; set; }

        protected ConfigurationHub Config { get; set; }

        public RebuildWarehouseIndexesJob(ConfigurationHub config)
        {
            Config = config;
        }

        protected internal override async Task Execute()
        {
            // Load default data if not provided
            WarehouseConnection = WarehouseConnection ?? Config.Sql.GetConnectionString(KnownSqlConnection.Warehouse);

            using (var connection = await WarehouseConnection.ConnectTo())
            {
                Log.RebuildingIndexes(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog);
                if (!WhatIf)
                {
                    await connection.ExecuteAsync("RebuildIndexes");
                }
                Log.RebuiltIndexes(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog);
            }
        }        
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-RebuildWarehouseIndexes")]
    public class RebuildWarehouseIndexesJobEventSource : EventSource
    {
        public static readonly RebuildWarehouseIndexesJobEventSource Log = new RebuildWarehouseIndexesJobEventSource();
        private RebuildWarehouseIndexesJobEventSource() { }

        [Event(
            eventId: 1,
            Task = Tasks.RebuildingIndexes,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Rebuilding Indexes in {0}/{1}")]
        public void RebuildingIndexes(string server, string database) { WriteEvent(1, server, database); }

        [Event(
            eventId: 2,
            Task = Tasks.RebuildingIndexes,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Rebuilt Indexes in {0}/{1}")]
        public void RebuiltIndexes(string server, string database) { WriteEvent(2, server, database); }

        public static class Tasks
        {
            public const EventTask RebuildingIndexes = (EventTask)0x1;
        }
    }
}
