using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json.Linq;
using NuGet.Services.Client;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Helpers;
using NuGet.Services.Work.Jobs.Models;

namespace NuGet.Services.Work.Jobs
{
    public class GenerateSearchRankingsJob : JobHandler<GenerateSearchRankingsEventSource>
    {
        public static readonly string DefaultContainerName = "ng-search";
        public static readonly string ReportName = "data/rankings.v1.json";

        public SqlConnectionStringBuilder WarehouseConnection { get; set; }
        public CloudStorageAccount Destination { get; set; }
        public string DestinationContainerName { get; set; }
        public string OutputDirectory { get; set; }

        protected ConfigurationHub Config { get; set; }
        protected CloudBlobContainer DestinationContainer { get; set; }

        public GenerateSearchRankingsJob(ConfigurationHub config)
        {
            Config = config;
        }

        protected internal override async Task Execute()
        {
            LoadDefaults();

            if (!String.IsNullOrEmpty(OutputDirectory))
            {
                Log.GeneratingSearchRankingReport(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog, OutputDirectory);
            }
            else if (Destination != null)
            {
                Log.GeneratingSearchRankingReport(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog, (Destination.Credentials.AccountName + "/" + DestinationContainer.Name));
            }
            else
            {
                throw new InvalidOperationException(Strings.WarehouseJob_NoDestinationAvailable);
            }

            // Gather overall rankings
            Log.GatheringOverallRankings(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog);
            var overallData = await GatherOverallRankings();
            Log.GatheredOverallRankings(overallData.Count);

            // Get project types
            Log.GettingAvailableProjectTypes(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog);
            var projectTypes = await GetProjectTypes();
            Log.GotAvailableProjectTypes(projectTypes.Count);

            // Gather data by project type
            IDictionary<string, IList<SearchRankingEntry>> byProjectType = new Dictionary<string, IList<SearchRankingEntry>>();
            int count = 0;
            Log.GatheringProjectTypeRankings(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog);
            foreach (var projectType in projectTypes)
            {
                Log.GatheringProjectTypeRanking(WarehouseConnection.DataSource, WarehouseConnection.InitialCatalog, projectType);
                var data = await GatherProjectTypeRanking(projectType);
                Log.GatheredProjectTypeRanking(data.Count, projectType);
                count += data.Count;

                byProjectType.Add(projectType, data);
            }
            Log.GatheredProjectTypeRankings(count);

            // Generate the report
            var report = new SearchRankingReport()
            {
                Overall = overallData,
                ByProjectType = byProjectType
            };

            // Write the JSON blob
            if (!String.IsNullOrEmpty(OutputDirectory))
            {
                await WriteToFile(report);
            }
            else
            {
                await DestinationContainer.CreateIfNotExistsAsync();
                await WriteToBlob(report);
            }
        }

        private async Task WriteToFile(SearchRankingReport report)
        {
            string fullPath = Path.Combine(OutputDirectory, ReportName);
            string parentDir = Path.GetDirectoryName(fullPath);
            Log.WritingReportBlob(fullPath);
            if (!WhatIf)
            {
                if (!Directory.Exists(parentDir))
                {
                    Directory.CreateDirectory(parentDir);
                }
                if (File.Exists(fullPath))
                {
                    File.Delete(fullPath);
                }
                using (var writer = new StreamWriter(File.OpenWrite(fullPath)))
                {
                    await writer.WriteAsync(JsonFormat.Serialize(report));
                }
            }
            Log.WroteReportBlob(fullPath);
        }

        private async Task WriteToBlob(SearchRankingReport report)
        {
            var blob = DestinationContainer.GetBlockBlobReference(ReportName);
            Log.WritingReportBlob(blob.Uri.AbsoluteUri);
            if (!WhatIf)
            {
                blob.Properties.ContentType = MimeTypes.Json;
                await blob.UploadTextAsync(JsonFormat.Serialize(report));
            }
            Log.WroteReportBlob(blob.Uri.AbsoluteUri);
        }

        private void LoadDefaults()
        {
            WarehouseConnection = WarehouseConnection ?? Config.Sql.Warehouse;
            Destination = Destination ?? Config.Storage.Legacy;
            if (Destination != null)
            {
                DestinationContainer = Destination.CreateCloudBlobClient().GetContainerReference(
                    String.IsNullOrEmpty(DestinationContainerName) ? DefaultContainerName : DestinationContainerName);
            }
        }

        private async Task<IList<SearchRankingEntry>> GatherOverallRankings()
        {
            using (var connection = await WarehouseConnection.ConnectTo())
            {
                // Get the script
                var script = await ResourceHelpers.ReadResourceFile("NuGet.Services.Work.Jobs.Scripts.SearchRanking_Overall.sql");

                // Execute it and return the results
                return (await connection.QueryAsync<SearchRankingEntry>(script)).ToList();
            }
        }

        private async Task<IList<string>> GetProjectTypes()
        {
            using (var connection = await WarehouseConnection.ConnectTo())
            {
                // Execute the query and return the results
                return (await connection.QueryAsync<string>("SELECT ProjectTypes FROM Dimension_Project")).ToList();
            }
        }

        private async Task<IList<SearchRankingEntry>> GatherProjectTypeRanking(string projectType)
        {
            using (var connection = await WarehouseConnection.ConnectTo())
            {
                // Get the script
                var script = await ResourceHelpers.ReadResourceFile("NuGet.Services.Work.Jobs.Scripts.SearchRanking_ByProjectType.sql");

                // Execute it and return the results
                return (await connection.QueryAsync<SearchRankingEntry>(script, new { ProjectGuid = projectType })).ToList();
            }
        }
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-GenerateSearchRankings")]
    public class GenerateSearchRankingsEventSource : EventSource
    {
        public static readonly GenerateSearchRankingsEventSource Log = new GenerateSearchRankingsEventSource();
        private GenerateSearchRankingsEventSource() { }

        [Event(
            eventId: 1,
            Message = "Gathering Overall Rankings from {0}/{1}...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.GatheringOverallRankings)]
        public void GatheringOverallRankings(string dbServer, string db) { WriteEvent(1, dbServer, db); }

        [Event(
            eventId: 2,
            Message = "Gathered {0} rows of data.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.GatheringOverallRankings)]
        public void GatheredOverallRankings(int rows) { WriteEvent(2, rows); }

        [Event(
            eventId: 3,
            Message = "Gathering Project Type Rankings from {0}/{1}...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.GatheringProjectTypeRankings)]
        public void GatheringProjectTypeRankings(string dbServer, string db) { WriteEvent(3, dbServer, db); }

        [Event(
            eventId: 4,
            Message = "Gathered {0} rows of data for all project types.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.GatheringProjectTypeRankings)]
        public void GatheredProjectTypeRankings(int rows) { WriteEvent(4, rows); }

        [Event(
            eventId: 5,
            Message = "Gathering Project Type Rankings for '{2}' from {0}/{1}...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.GatheringProjectTypeRanking)]
        public void GatheringProjectTypeRanking(string dbServer, string db, string projectType) { WriteEvent(5, dbServer, db, projectType); }

        [Event(
            eventId: 6,
            Message = "Gathered {0} rows of data for project type '{1}'.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.GatheringProjectTypeRanking)]
        public void GatheredProjectTypeRanking(int rows, string projectType) { WriteEvent(6, rows, projectType); }

        [Event(
            eventId: 7,
            Message = "Writing report to {0}.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.WritingReportBlob)]
        public void WritingReportBlob(string uri) { WriteEvent(7, uri); }

        [Event(
            eventId: 8,
            Message = "Wrote report to {0}.",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.WritingReportBlob)]
        public void WroteReportBlob(string uri) { WriteEvent(8, uri); }

        [Event(
            eventId: 9,
            Message = "Getting Project Types from {0}/{1}...",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Start,
            Task = Tasks.GettingAvailableProjectTypes)]
        public void GettingAvailableProjectTypes(string dbServer, string db) { WriteEvent(9, dbServer, db); }

        [Event(
            eventId: 10,
            Message = "Got {0} project types",
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Stop,
            Task = Tasks.GettingAvailableProjectTypes)]
        public void GotAvailableProjectTypes(int rows) { WriteEvent(10, rows); }

        [Event(
            eventId: 11,
            Message = "Generating Search Ranking Report from {0}/{1} to {2}.",
            Level = EventLevel.Informational)]
        public void GeneratingSearchRankingReport(string dbServer, string db, string destinaton) { WriteEvent(11, dbServer, db, destinaton); }

        public static class Tasks
        {
            public const EventTask GatheringOverallRankings = (EventTask)0x01;
            public const EventTask GatheringProjectTypeRankings = (EventTask)0x02;
            public const EventTask GatheringProjectTypeRanking = (EventTask)0x03;
            public const EventTask WritingReportBlob = (EventTask)0x04;
            public const EventTask GettingAvailableProjectTypes = (EventTask)0x05;
        }
    }
}
