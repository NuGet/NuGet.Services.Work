// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NuGet.Services.Configuration;

namespace NuGet.Services.Work.Jobs
{
    public class GenerateDownloadCountReportJob : ReportGeneratingJobBase<GenerateDownloadCountReportEventSource>
    {
        private const string GetDownloadsScript = @"-- Work Service / GenerateDownloadCountReport / GetDownloadsScript
            SELECT p.[Key] AS PackageKey, pr.Id, p.NormalizedVersion, p.DownloadCount, pr.DownloadCount AS 'AllVersionsDownloadCount'
            FROM Packages p WITH (NOLOCK)
            INNER JOIN PackageRegistrations pr ON p.PackageRegistrationKey = pr.[Key]";
        private const string GetRecentDataScript = @"-- Work Service / GenerateDownloadCountReport / GetRecentDataScript
            DECLARE @Install int
            DECLARE @Update int

            -- Get the IDs of the Operations we're interested in
            SELECT @Install = [Id] FROM Dimension_Operation WHERE Operation = 'Install';
            SELECT @Update = [Id] FROM Dimension_Operation WHERE Operation = 'Update';

            -- Group data by Dimension_Package_Id and stuff it in a table variable
            DECLARE @temp TABLE(
	            Dimension_Package_Id int,
	            InstallCount int,
	            UpdateCount int);

            WITH cte AS(
	            SELECT
		            Dimension_Package_Id, 
		            (CASE WHEN Dimension_Operation_Id = @Install THEN 1 ELSE 0 END) AS [Install],
		            (CASE WHEN Dimension_Operation_Id = @Update THEN 1 ELSE 0 END) AS [Update]
	            FROM Fact_Download WITH(NOLOCK)
	            WHERE Dimension_Operation_Id = @Install OR Dimension_Operation_Id = @Update 
            )
            INSERT INTO @temp(Dimension_Package_Id, InstallCount, UpdateCount)
            SELECT
	            [Dimension_Package_Id],
	            SUM([Install]) AS InstallCount, 
	            SUM([Update]) AS UpdateCount
            FROM cte
            INNER JOIN Dimension_Package ON Dimension_Package.Id = cte.Dimension_Package_Id
            GROUP BY Dimension_Package_Id

            -- Get the actual PackageId and PackageVersion using the Dimension_Package_Id in the table variable
            SELECT Dimension_Package.PackageId, Dimension_Package.PackageVersion, t.InstallCount, t.UpdateCount
            FROM @temp t
            INNER JOIN Dimension_Package ON Dimension_Package.Id = t.Dimension_Package_Id";

        public static readonly string DefaultContainerName = "ng-search-data";
        public static readonly string ReportName = "downloads.v1.json";

        public SqlConnectionStringBuilder WarehouseConnection { get; set; }
        public SqlConnectionStringBuilder PackagesConnection { get; set; }

        public GenerateDownloadCountReportJob(ConfigurationHub config) : base(config, DefaultContainerName) {}

        protected override async Task ExecuteCore()
        {
            string destination = String.IsNullOrEmpty(OutputDirectory) ?
                (Destination.Credentials.AccountName + "/" + DestinationContainer.Name) :
                OutputDirectory;
            if (String.IsNullOrEmpty(destination))
            {
                throw new Exception(Strings.WarehouseJob_NoDestinationAvailable);
            }
            
            Log.GeneratingDownloadCountReport(PackagesConnection.DataSource, PackagesConnection.InitialCatalog, WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog, destination);

            // Gather download count data from packages database
            IList<DownloadCountData> downloadData;
            Log.GatheringDownloadCounts(PackagesConnection.DataSource, PackagesConnection.InitialCatalog);
            using (var connection = await PackagesConnection.ConnectTo())
            {
                downloadData = (await connection.QueryWithRetryAsync<DownloadCountData>(GetDownloadsScript)).ToList();
            }
            Log.GatheredDownloadCounts(downloadData.Count);

            // Gather recent activity data from warehouse
            IList<RecentActivityData> recentActivityData;
            Log.GatheringRecentActivityCounts(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog);
            using (var connection = await WarehouseConnection.ConnectTo())
            {
                recentActivityData = (await connection.QueryWithRetryAsync<RecentActivityData>(GetRecentDataScript)).ToList();
            }
            Log.GatheredRecentActivityCounts(recentActivityData.Count);

            // Join!
            Log.JoiningData();
            IDictionary<int, FullDownloadData> data =
                downloadData.GroupJoin(
                    recentActivityData,
                    dcd => GetKey(dcd.Id, dcd.NormalizedVersion),
                    rad => GetKey(rad.PackageId, rad.PackageVersion),

                    (dcd, rads) => new {
                        Key = dcd.PackageKey, 
                        Value = new FullDownloadData()
                        {
                            Id = dcd.Id,
                            Version = dcd.NormalizedVersion,
                            Downloads = dcd.DownloadCount,
                            RegistrationDownloads = dcd.AllVersionsDownloadCount,
                            Installs = rads.Any() ? rads.Sum(r => r.InstallCount) : 0, 
                            Updates = rads.Any() ? rads.Sum(r => r.UpdateCount) : 0
                        }
                    },
                    StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(pair => pair.Key, pair => pair.Value);
            Log.JoinedData();

            // Write the report
            await WriteReport(JObject.FromObject(data), ReportName, Log.WritingReportBlob, Log.WroteReportBlob, Formatting.None);
        }

        private string GetKey(string id, string version)
        {
            return (id + "/" + SemanticVersionHelper.Normalize(version)).ToLower();
        }

        protected override void LoadDefaults()
        {
            base.LoadDefaults();
            WarehouseConnection = WarehouseConnection ?? Config.Sql.Warehouse;
            PackagesConnection = PackagesConnection ?? Config.Sql.Legacy;
        }

        public class DownloadCountData
        {
            public int PackageKey { get; set; }
            public string Id { get; set; }
            public string NormalizedVersion { get; set; }
            public int DownloadCount { get; set; }
            public int AllVersionsDownloadCount { get; set; }
        }

        public class RecentActivityData
        {
            public string PackageId { get; set; }
            public string PackageVersion { get; set; }
            public int InstallCount { get; set; }
            public int UpdateCount { get; set; }
        }

        public class FullDownloadData
        {
            public string Id { get; set; }
            public string Version { get; set; }
            public int Downloads { get; set; }
            public int RegistrationDownloads { get; set; }
            public int Installs { get; set; }
            public int Updates { get; set; }
        }
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-GenerateDownloadCountReport")]
    public class GenerateDownloadCountReportEventSource : EventSource
    {
        public static readonly GenerateDownloadCountReportEventSource Log = new GenerateDownloadCountReportEventSource();
        private GenerateDownloadCountReportEventSource() { }

        [Event(
            eventId: 1,
            Message = "Writing report to {0}.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.WritingReportBlob)]
        public void WritingReportBlob(string uri) { WriteEvent(1, uri); }

        [Event(
            eventId: 2,
            Message = "Wrote report to {0}.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.WritingReportBlob)]
        public void WroteReportBlob(string uri) { WriteEvent(2, uri); }

        [Event(
            eventId: 3,
            Message = "Generating Download Count Report from {0}/{1} and {2}/{3} to {4}.",
            Level = EventLevel.Informational)]
        public void GeneratingDownloadCountReport(string packageServer, string packageDb, string warehouseServer, string warehouseDb, string destinaton) { WriteEvent(3, packageServer, packageDb, warehouseServer, warehouseDb, destinaton); }

        [Event(
            eventId: 4,
            Message = "Gathering Download Counts from {0}/{1}...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.GatheringDownloadCounts)]
        public void GatheringDownloadCounts(string dbServer, string db) { WriteEvent(4, dbServer, db); }

        [Event(
            eventId: 5,
            Message = "Gathered {0} rows of data.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.GatheringDownloadCounts)]
        public void GatheredDownloadCounts(int rows) { WriteEvent(5, rows); }

        [Event(
            eventId: 6,
            Message = "Gathering Recent Activity Counts from {0}/{1}...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.GatheringRecentActivityCounts)]
        public void GatheringRecentActivityCounts(string dbServer, string db) { WriteEvent(6, dbServer, db); }

        [Event(
            eventId: 7,
            Message = "Gathered {0} rows of data.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.GatheringRecentActivityCounts)]
        public void GatheredRecentActivityCounts(int rows) { WriteEvent(7, rows); }

        [Event(
            eventId: 9,
            Message = "Joining data in memory...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.CombiningData)]
        public void JoiningData() { WriteEvent(9); }

        [Event(
            eventId: 10,
            Message = "Joined data.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.CombiningData)]
        public void JoinedData() { WriteEvent(10); }

        public static class Tasks
        {
            public const EventTask GatheringDownloadCounts = (EventTask)0x1;
            public const EventTask GatheringRecentActivityCounts = (EventTask)0x2;
            public const EventTask CombiningData = (EventTask)0x3;
            public const EventTask WritingReportBlob = (EventTask)0x4;
        }
    }
}
