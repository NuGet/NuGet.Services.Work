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
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Management.Sql.Models;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Azure;
using NuGet.Services.Work.Helpers;
using NuGet.Services.Work.Jobs.Models;

namespace NuGet.Services.Work.Jobs
{
    [Description("Cleans databases based on a provided policy")]
    public class CleanDatabasesJob : AsyncJobHandler<CleanDatabasesEventSource>
    {
        /// <summary>
        /// The name of the server to clean from
        /// </summary>
        public string ServerName { get; set; }
        
        /// <summary>
        /// The prefix to apply to the backup
        /// </summary>
        public string NamePrefix { get; set; }

        /// <summary>
        /// The maximum number of running copies to keep
        /// </summary>
        public int? MaxRunningCopies { get; set; }

        /// <summary>
        /// The maximum number of daily copies to keep (includes "today", so to keep today's last backup and yesterday's, specify 2)
        /// </summary>
        public int? MaxDailyCopies { get; set; }

        protected AzureHub Azure { get; set; }
        protected ConfigurationHub Config { get; set; }
        
        public CleanDatabasesJob(AzureHub azure, ConfigurationHub config)
        {
            Azure = azure;
            Config = config;
        }

        protected internal override async Task<JobContinuation> Execute()
        {
            ServerName = String.IsNullOrEmpty(ServerName) ? Utils.GetSqlServerName(Config.Sql.Legacy.DataSource) : ServerName;
            
            // Capture the current time in case the date changes during invocation
            DateTimeOffset now = DateTimeOffset.UtcNow;
            
            // Resolve the connection if not specified explicitly
            Log.PreparingToClean(ServerName);

            // Connect to the master database
            using(var sql = CloudContext.Clients.CreateSqlManagementClient(Azure.GetCredentials(throwIfMissing: true)))
            {
                // Get online databases
                Log.GettingDatabaseList(ServerName);
                var dbs = (await sql.Databases.ListAsync(ServerName)).ToList();
                Log.GotDatabases(dbs.Count, ServerName);

                // Determine which of these are backups
                var backups = dbs
                    .Select(d => DatabaseBackup<Database>.Create(d))
                    .Where(b => b != null && String.Equals(NamePrefix, b.Prefix, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                // Start collecting a list of backups we're going to keep
                var keepers = new HashSet<DatabaseBackup<Database>>();

                // Group backups by UTC Day
                var backupsByDate = backups
                    .GroupBy(b => b.Timestamp.UtcDateTime.Date)
                    .OrderByDescending(g => g.Key);

                // Keep the last backup from today and the max daily backups if any
                var dailyBackups = backupsByDate
                    .Take(MaxDailyCopies ?? 1)
                    .Select(g => g.OrderBy(db => db.Timestamp).Last());
                foreach (var keeper in dailyBackups)
                {
                    keepers.Add(keeper);
                }

                // Keep the most recent backups based on MaxRunningBackups
                foreach (var keeper in backups.OrderByDescending(b => b.Timestamp).Take(MaxRunningCopies ?? 1))
                {
                    keepers.Add(keeper);
                }

                // Report keepers
                foreach (var keeper in keepers)
                {
                    Log.KeepingBackup(keeper.Db.Name);
                }

                // Delete the others!
                foreach (var db in backups.Select(b => b.Db.Name).Except(keepers.Select(b => b.Db.Name), StringComparer.OrdinalIgnoreCase))
                {
                    Log.DeletingBackup(db);
                    if (!WhatIf)
                    {
                        await sql.Databases.DeleteAsync(ServerName, db);
                    }
                    Log.DeletedBackup(db);
                }

                // Clean out CopyTemp databases older than 3 hours
                var copytemps = dbs
                    .Where(db =>
                        db.Name.StartsWith("copytemp", StringComparison.OrdinalIgnoreCase) &&
                        db.CreationDate < DateTime.UtcNow.AddHours(-3));
                foreach (var copytemp in copytemps)
                {
                    Log.DeletingBackup(copytemp.Name);
                    if(!WhatIf)
                    {
                        await sql.Databases.DeleteAsync(ServerName, copytemp.Name);
                    }
                    Log.DeletedBackup(copytemp.Name);
                }
            }
            return Complete();
        }
    }

    [EventSource(Name="Outercurve-NuGet-Jobs-CleanDatabases")]
    public class CleanDatabasesEventSource : EventSource
    {
        public static readonly CleanDatabasesEventSource Log = new CleanDatabasesEventSource();
        
        private CleanDatabasesEventSource() { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Preparing to clean backups on {0}")]
        public void PreparingToClean(string server) { WriteEvent(1, server); }

        [Event(
            eventId: 2,
            Task = Tasks.GetDatabases,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Getting list of databases on {0}")]
        public void GettingDatabaseList(string server) { WriteEvent(2, server); }

        [Event(
            eventId: 3,
            Task = Tasks.GetDatabases,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Retrieved {0} ONLINE databases on {1}")]
        public void GotDatabases(int count, string server) { WriteEvent(3, count, server); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Message = "Keeping database: {0}")]
        public void KeepingBackup(string database) { WriteEvent(4, database); }

        [Event(
            eventId: 5,
            Task = Tasks.DeleteDatabase,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Deleting database: {0}")]
        public void DeletingBackup(string database) { WriteEvent(5, database); }

        [Event(
            eventId: 6,
            Task = Tasks.DeleteDatabase,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Deleted database: {0}")]
        public void DeletedBackup(string database) { WriteEvent(6, database); }

        public class Tasks
        {
            public const EventTask GetDatabases = (EventTask)0x1;
            public const EventTask DeleteDatabase = (EventTask)0x2;
        }
    }
}
