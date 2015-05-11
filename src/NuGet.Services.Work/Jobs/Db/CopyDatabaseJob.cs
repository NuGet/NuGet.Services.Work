// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.ComponentModel;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Management.Sql;
using Microsoft.WindowsAzure.Management.Sql.Models;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Azure;
using NuGet.Services.Work.Helpers;

namespace NuGet.Services.Work.Jobs
{
    [Description("Copies an Azure SQL Database.")]
    public class CopyDatabaseJob : AsyncJobHandler<RestoreDatabaseBackupEventSource>
    {
        public string SourceServerName { get; set; }
        public string SourceDatabaseName { get; set; }
        public string TargetServerName { get; set; }

        public string TargetDatabaseName { get; set; }
        public string TargetDatabaseNamePrefix { get; set; }

        public TimeSpan? Timeout { get; set; }
        public DateTime? Start { get; set; }

        public string CopyOperationId { get; set; }
        public string CopyName { get; set; }

        protected AzureHub Azure { get; set; }
        protected ConfigurationHub Config { get; set; }

        public CopyDatabaseJob(AzureHub azure, ConfigurationHub config)
        {
            Azure = azure;
            Config = config;
            AddEventSource(AzureHubEventSource.Log);
        }

        protected internal override async Task<JobContinuation> Execute()
        {
            // Give an extra 10 minutes to execute
            await Extend(TimeSpan.FromMinutes(10));

            Start = DateTime.UtcNow;

            // Defaults:
            //  SourceServerName = Sql.Legacy Server Name
            //  SourceDatabaseName = Sql.Legacy DB Name
            //  TargetServerName = SourceServerName
            SourceServerName = String.IsNullOrEmpty(SourceServerName) ? Utils.GetSqlServerName(Config.Sql.Legacy.DataSource) : SourceServerName;
            SourceDatabaseName = String.IsNullOrEmpty(SourceDatabaseName) ? Config.Sql.Legacy.InitialCatalog : SourceDatabaseName;
            TargetServerName = String.IsNullOrEmpty(TargetServerName) ? SourceServerName : TargetServerName;

            TargetDatabaseName = String.IsNullOrEmpty(TargetDatabaseNamePrefix) ?
                TargetDatabaseName :
                (TargetDatabaseNamePrefix + "_" + DateTime.UtcNow.ToString("yyyyMMMdd_HHmm") + "Z").ToLowerInvariant();

            // Use our invocation ID to generate a unique name
            CopyName = "copytemp_" + Context.Invocation.Id.ToString("N");

            Log.BeginningDatabaseCopyProcess(SourceServerName, SourceDatabaseName, TargetServerName, TargetDatabaseName, CopyName);

            using (var sql = CloudContext.Clients.CreateSqlManagementClient(Azure.GetCredentials(throwIfMissing: true)))
            {
                // 1. Start the copy to a unique-named database

                Log.StartingCopy(SourceServerName, SourceDatabaseName, TargetServerName, CopyName);
                var response = await sql.DatabaseCopies.CreateAsync(
                    SourceServerName, 
                    SourceDatabaseName,
                    new DatabaseCopyCreateParameters()
                    {
                        PartnerDatabase = CopyName,
                        PartnerServer = TargetServerName
                    });

                // Would have thrown if it failed, so now start checking the status
                CopyOperationId = response.DatabaseCopy.Name;
                Log.StartedCopy(CopyOperationId);

                return await Resume();
            }
        }

        protected internal override async Task<JobContinuation> Resume()
        {
            // Give us another 10 minutes to execute
            await Extend(TimeSpan.FromMinutes(10));

            using (var sql = CloudContext.Clients.CreateSqlManagementClient(Azure.GetCredentials(throwIfMissing: true)))
            {
                try
                {
                    // 2. Check the status of the copy
                    Log.CheckingCopyStatus(CopyOperationId);
                    var ops = await sql.DatabaseOperations.ListByDatabaseAsync(TargetServerName, CopyName);
                    var op = ops.FirstOrDefault();
                    switch (op.StateId)
                    {
                        case 2: // COMPLETED (http://msdn.microsoft.com/en-us/library/azure/dn720371.aspx)
                            Log.CopyCompleted();
                            await CompleteCopy(sql);
                            Log.CompletedDatabaseCopyProcess(SourceServerName, SourceDatabaseName, TargetServerName, TargetDatabaseName);
                            return Complete();
                        case 3: // FAILED
                            // Copy failed! Fail the whole job
                            throw new JobFailureException(op.Error);
                        default:
                            // Copy is still in progress, check for timeout
                            if (Timeout.HasValue && ((DateTime.UtcNow - Context.Invocation.QueuedAt.UtcDateTime) >= Timeout.Value))
                            {
                                // Abort the copy
                                await AbortCopy(sql);
                                throw new JobFailureException("Copy operation exceeded timeout and was aborted.");
                            }

                            // Save state and wait for another five minutes
                            Log.CopyInProgress(op.PercentComplete);
                            return Suspend(TimeSpan.FromMinutes(5), new
                            {
                                SourceServerName,
                                SourceDatabaseName,
                                TargetServerName,
                                TargetDatabaseName,
                                CopyName,
                                CopyOperationId,
                                Timeout,
                                Start
                            });
                    }
                }
                catch (Exception)
                {
                    // Abort the copy
                    AbortCopy(sql).Wait();

                    throw;
                }
            }
        }

        private async Task AbortCopy(SqlManagementClient sql)
        {
            Log.AbortingCopy(SourceServerName, SourceDatabaseName);
            await sql.Databases.DeleteAsync(TargetServerName, CopyName);
            Log.AbortedCopy(SourceServerName, SourceDatabaseName);
        }

        private async Task CompleteCopy(SqlManagementClient sql)
        {
            // 3. Delete the target if it exists
            Log.CheckingForExistingCopyTarget(TargetDatabaseName);
            var existingDatabases = await sql.Databases.ListAsync(TargetServerName);
            var existingDb = existingDatabases.FirstOrDefault(db => String.Equals(db.Name, TargetDatabaseName, StringComparison.OrdinalIgnoreCase));
            string existingBackupName = null;
            if (existingDb != null)
            {
                // We need to do a swap
                Log.CopyTargetExists();

                // 4. Generate a name for the old restore target and rename to it
                existingBackupName = TargetDatabaseName + "_swap_" + Invocation.Id.ToString("N");
                Log.RenamingExistingCopyTarget(TargetDatabaseName, existingBackupName);
                await sql.Databases.UpdateAsync(TargetServerName, TargetDatabaseName, new DatabaseUpdateParameters()
                {
                    Edition = existingDb.Edition,
                    Name = existingBackupName
                });
                Log.RenamedExistingCopyTarget();
            }
            else
            {
                Log.CopyTargetDoesNotExist();
            }

            ExceptionDispatchInfo error = null;
            try
            {
                // 5. Now rename the target
                Log.RenamingNewCopy(CopyName, TargetDatabaseName);
                var restoreDb = await sql.Databases.GetAsync(TargetServerName, CopyName);
                await sql.Databases.UpdateAsync(TargetServerName, CopyName, new DatabaseUpdateParameters()
                {
                    Edition = restoreDb.Database.Edition,
                    Name = TargetDatabaseName
                });
                Log.RenamedNewCopy();
            }
            catch (Exception ex)
            {
                if (existingDb == null)
                {
                    throw;
                }
                error = ExceptionDispatchInfo.Capture(ex);
            }

            if (existingDb != null)
            {
                if (error != null)
                {
                    // 6. Recover the old export
                    Log.RecoveringExistingCopy(existingBackupName, TargetDatabaseName);
                    await sql.Databases.UpdateAsync(TargetServerName, CopyName, new DatabaseUpdateParameters()
                    {
                        Edition = existingDb.Edition,
                        Name = TargetDatabaseName
                    });
                    Log.RecoveredOldCopy();
                    error.Throw();
                }
                else
                {
                    // 7. Delete the old export
                    Log.DeletingOldCopy(existingBackupName);
                    await sql.Databases.DeleteAsync(TargetServerName, existingBackupName);
                    Log.DeletedOldCopy();
                }
            }
        }
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-RestoreDatabaseBackup")]
    public class RestoreDatabaseBackupEventSource : EventSource
    {
        public static readonly RestoreDatabaseBackupEventSource Log = new RestoreDatabaseBackupEventSource();
        private RestoreDatabaseBackupEventSource() { }

        [Event(
            eventId: 1,
            Message = "Starting copy of {0}/{1} to {2}/{3}",
            Task = Tasks.StartingCopy,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational)]
        public void StartingCopy(string sourceServer, string sourceDb, string targetServer, string targetDb) { WriteEvent(1, sourceServer, sourceDb, targetServer, targetDb); }

        [Event(
            eventId: 2,
            Message = "Started restore. Operation ID: {0}",
            Task = Tasks.StartingCopy,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational)]
        public void StartedCopy(string operationId) { WriteEvent(2, operationId); }

        [Event(
            eventId: 3,
            Message = "Checking status of restore {0}",
            Task = Tasks.CheckingCopyStatus,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational)]
        public void CheckingCopyStatus(string operationId) { WriteEvent(3, operationId); }

        [Event(
            eventId: 4,
            Message = "Copy is still in progress: {0}% complete.",
            Task = Tasks.CheckingCopyStatus,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational)]
        public void CopyInProgress(int percentComplete) { WriteEvent(4, percentComplete); }

        [Event(
            eventId: 5,
            Message = "Copy completed!",
            Task = Tasks.CheckingCopyStatus,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational)]
        public void CopyCompleted() { WriteEvent(5); }


        [Event(
            eventId: 6,
            Task = Tasks.CheckingForExistingCopyTarget,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Checking if the copy target '{0}' exists already.")]
        public void CheckingForExistingCopyTarget(string targetName) { WriteEvent(6, targetName); }

        [Event(
            eventId: 7,
            Task = Tasks.CheckingForExistingCopyTarget,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Copy target exists, doing a swap.")]
        public void CopyTargetExists() { WriteEvent(7); }

        [Event(
            eventId: 8,
            Task = Tasks.CheckingForExistingCopyTarget,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Copy target does not exist, doing a rename.")]
        public void CopyTargetDoesNotExist() { WriteEvent(8); }

        [Event(
            eventId: 9,
            Task = Tasks.RenamingExistingCopyTarget,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Renaming existing database '{0}' to '{1}'")]
        public void RenamingExistingCopyTarget(string existingName, string newName) { WriteEvent(9, existingName, newName); }

        [Event(
            eventId: 10,
            Task = Tasks.RenamingExistingCopyTarget,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Rename complete.")]
        public void RenamedExistingCopyTarget() { WriteEvent(10); }

        [Event(
            eventId: 11,
            Task = Tasks.RenamingNewCopy,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Renaming '{0}' to '{1}'")]
        public void RenamingNewCopy(string existingName, string newName) { WriteEvent(11, existingName, newName); }

        [Event(
            eventId: 12,
            Task = Tasks.RenamingNewCopy,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Rename complete.")]
        public void RenamedNewCopy() { WriteEvent(12); }

        [Event(
            eventId: 13,
            Task = Tasks.DeletingOldCopy,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Deleting swapped-out database '{0}'")]
        public void DeletingOldCopy(string existingName) { WriteEvent(13, existingName); }

        [Event(
            eventId: 14,
            Task = Tasks.DeletingOldCopy,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Delete complete.")]
        public void DeletedOldCopy() { WriteEvent(14); }

        [Event(
            eventId: 15,
            Task = Tasks.RecoveringExistingCopy,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Something went wrong, restoring swap-out backup '{0}' to '{1}'.")]
        public void RecoveringExistingCopy(string existingName, string newName) { WriteEvent(15, existingName, newName); }

        [Event(
            eventId: 16,
            Task = Tasks.RecoveringExistingCopy,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Recovery complete.")]
        public void RecoveredOldCopy() { WriteEvent(16); }

        [Event(
            eventId: 17,
            Task = Tasks.DatabaseCopyProcess,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Starting copy of {0}/{1} to {2}/{3} via temporary database name: {4}")]
        public void BeginningDatabaseCopyProcess(string sourceServer, string sourceDatabase, string targetServer, string targetDatabase, string copyName) {  WriteEvent(17, sourceServer, sourceDatabase, targetServer, targetDatabase, copyName); }

        [Event(
            eventId: 18,
            Task = Tasks.DatabaseCopyProcess,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Completed copy of {0}/{1} to {2}/{3}!")]
        public void CompletedDatabaseCopyProcess(string sourceServer, string sourceDatabase, string targetServer, string targetDatabase) { WriteEvent(18, sourceServer, sourceDatabase, targetServer, targetDatabase); }

        [Event(
            eventId: 19,
            Task = Tasks.AbortingCopy,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Error,
            Message = "Timeout elapsed! Aborting copy of {0}/{1}!")]
        public void AbortingCopy(string sourceServer, string sourceDatabase) { WriteEvent(19, sourceServer, sourceDatabase); }

        [Event(
            eventId: 20,
            Task = Tasks.AbortingCopy,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Error,
            Message = "Aborted copy of {0}/{1}!")]
        public void AbortedCopy(string sourceServer, string sourceDatabase) { WriteEvent(20, sourceServer, sourceDatabase); }

        public static class Tasks
        {
            public const EventTask StartingCopy = (EventTask)0x1;
            public const EventTask CheckingCopyStatus = (EventTask)0x2;
            public const EventTask CheckingExistingRestores = (EventTask)0x3;
            public const EventTask CheckingForExistingCopyTarget = (EventTask)0x4;
            public const EventTask RenamingExistingCopyTarget = (EventTask)0x5;
            public const EventTask RenamingNewCopy = (EventTask)0x6;
            public const EventTask DeletingOldCopy = (EventTask)0x7;
            public const EventTask RecoveringExistingCopy = (EventTask)0x8;
            public const EventTask DatabaseCopyProcess = (EventTask)0x9;
            public const EventTask AbortingCopy = (EventTask)0xA;
        }
    }
}
