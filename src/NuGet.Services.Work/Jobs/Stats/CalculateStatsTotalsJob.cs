// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NuGet.Indexing;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Jobs.Bases;
using NuGet.Services.Work.Monitoring;

namespace NuGet.Services.Work.Jobs
{
    [Description("Calculates the unique and total package counts and gets the total download count from SQL")]
    public class CalculateStatsTotalsJob : JobHandler<CaclculateStatsTotalsEventSource>
    {
        protected ConfigurationHub Config { get; set; }

        // Note the NOLOCK hints here!
        private static readonly string GetStatisticsSql = @"SELECT 
                    (SELECT COUNT([Key]) FROM PackageRegistrations pr WITH (NOLOCK)
                            WHERE EXISTS (SELECT 1 FROM Packages p WITH (NOLOCK) WHERE p.PackageRegistrationKey = pr.[Key] AND p.Listed = 1)) AS UniquePackages,
                    (SELECT COUNT([Key]) FROM Packages WITH (NOLOCK) WHERE Listed = 1) AS TotalPackages,
                    (SELECT TotalDownloadCount FROM GallerySettings WITH (NOLOCK)) AS Downloads";

        public CalculateStatsTotalsJob(ConfigurationHub config)
        {
            Config = config;
        }

        protected internal override async Task Execute()
        {
            var contentAccount = Config.Storage.Legacy;
            var contentContainerName = "content";
            var contentContainer = contentAccount.CreateCloudBlobClient().GetContainerReference(contentContainerName);

            var packageDatabase = Config.Sql.Legacy;

            Totals totals;
            Log.BeginningQuery(packageDatabase.DataSource, packageDatabase.InitialCatalog);
            using (var connection = await packageDatabase.ConnectTo())
            {
                totals = (await connection.QueryAsync<Totals>(GetStatisticsSql)).SingleOrDefault();
            }

            if (totals == null)
            {
                throw new Exception(Strings.CalculateStatsTotalsJob_NoData);
            }

            Log.FinishedQuery(totals.UniquePackages, totals.TotalPackages, totals.Downloads, totals.LastUpdateDateUtc);

            string name = "stats-totals.json";
            Log.BeginningBlobUpload(name);
            await contentContainer.UploadJsonBlob(name, totals);
            Log.FinishedBlobUpload();
        }

        public class Totals
        {
            public int UniquePackages { get; set; }
            public int TotalPackages { get; set; }
            public int Downloads { get; set; }

            public DateTime LastUpdateDateUtc { get { return DateTime.UtcNow; } }
        }
    }

    [EventSource(Name="Outercurve-NuGet-Jobs-CalculateStatsTotals")]
    public class CaclculateStatsTotalsEventSource : EventSource
    {
        public static readonly CaclculateStatsTotalsEventSource Log = new CaclculateStatsTotalsEventSource();
        private CaclculateStatsTotalsEventSource() { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Begining the query of the database to get statistics from {0}/{1}",
            Task = Tasks.Querying,
            Opcode = EventOpcode.Start)]
        public void BeginningQuery(string server, string database) { WriteEvent(1, server, database); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Message = "Finished querying the database. Unique Packages: {0}, Total Packages: {1}, Download Count: {2}, Last Updated Date UTC: {3}",
            Task = Tasks.Querying,
            Opcode = EventOpcode.Stop)]
        public void FinishedQuery(int uniquePackages, int totalPackages, int downloadCount, DateTime lastUpdatedUtc)
        {
            WriteEvent(2, uniquePackages, totalPackages, downloadCount, lastUpdatedUtc);
        }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Message = "Beginning blob upload: {0}",
            Task = Tasks.Uploading,
            Opcode = EventOpcode.Start)]
        public void BeginningBlobUpload(string blobName) { WriteEvent(3, blobName); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Message = "Finished blob upload",
            Task = Tasks.Uploading,
            Opcode = EventOpcode.Stop)]
        public void FinishedBlobUpload() { WriteEvent(4); }

        public static class Tasks
        {
            public const EventTask Querying = (EventTask)0x1;
            public const EventTask Uploading = (EventTask)0x2;
        }
    }
}
