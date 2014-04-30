using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Management.Sql;
using Microsoft.WindowsAzure.Management.Sql.Models;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Azure;

namespace NuGet.Services.Work.Jobs
{
    [Description("Materializes a SQL Azure Database backup as a real online database.")]
    public class RestoreDatabaseBackupJob : AsyncJobHandler<RestoreDatabaseBackupEventSource>
    {
        /// <summary>
        /// Gets or sets the age of the backup to restore
        /// </summary>
        public TimeSpan BackupAge { get; set; }

        /// <summary>
        /// Gets or sets the specific backup time to restore from. This overrides BackupAge
        /// </summary>
        public DateTime? BackupTimeUtc { get; set; }

        /// <summary>
        /// Gets or sets the name of the SQL Server containing the source database
        /// </summary>
        public string SourceServerName { get; set; }

        /// <summary>
        /// Gets or sets the name of the SQL Server to restore the backup to
        /// </summary>
        public string TargetServerName { get; set; }

        /// <summary>
        /// Gets or sets the source database name
        /// </summary>
        public string SourceDatabaseName { get; set; }

        /// <summary>
        /// Gets or sets the name to give the restored database.
        /// </summary>
        public string TargetDatabaseName { get; set; }

        public string RestoreOperationId { get; set; }
        public string RestoreName { get; set; }

        protected AzureHub Azure { get; set; }
        protected ConfigurationHub Config { get; set; }

        public RestoreDatabaseBackupJob(AzureHub azure)
        {
            Azure = azure;
            AddEventSource(AzureHubEventSource.Log);
        }

        protected internal override async Task<JobContinuation> Execute()
        {
            var backupTime = BackupTimeUtc ?? (DateTime.UtcNow - BackupAge);

            // Defaults:
            //  SourceServerName = Sql.Legacy Server Name
            //  SourceDatabaseName = Sql.Legacy DB Name
            //  TargetServerName = SourceServerName
            SourceServerName = String.IsNullOrEmpty(SourceServerName) ? GetServerName(Config.Sql.Legacy.DataSource) : SourceServerName;
            SourceDatabaseName = String.IsNullOrEmpty(SourceDatabaseName) ? Config.Sql.Legacy.InitialCatalog : SourceDatabaseName;
            TargetServerName = String.IsNullOrEmpty(TargetServerName) ? SourceServerName : TargetServerName;

            RestoreName = TargetDatabaseName + DateTime.UtcNow.ToString("_yyyyMMddHHmmss");

            using (var sql = CloudContext.Clients.CreateSqlManagementClient(Azure.GetCredentials()))
            {
                Log.StartingRestore(SourceServerName, SourceDatabaseName, TargetServerName, RestoreName, backupTime.ToString("O"));
                var response = await sql.RestoreDatabaseOperations.CreateAsync(
                    SourceServerName, new RestoreDatabaseOperationCreateParameters()
                    {
                        PointInTime = backupTime,
                        SourceDatabaseName = SourceDatabaseName,
                        TargetDatabaseName = RestoreName,
                        TargetServerName = TargetServerName
                    });

                // Would have thrown if it failed, so now start checking the status
                RestoreOperationId = response.Operation.Id;
                Log.StartedRestore(RestoreOperationId);

                return await Resume();
            }
        }

        protected internal override async Task<JobContinuation> Resume()
        {
            // Check the status of the operation
            using (var sql = CloudContext.Clients.CreateSqlManagementClient(Azure.GetCredentials()))
            {
                Log.CheckingRestoreStatus(RestoreOperationId);
                var response = await sql.DatabaseOperations.GetAsync(TargetServerName, RestoreOperationId);
                switch (response.DatabaseOperation.State)
                {
                    case "COMPLETED":
                        Log.RestoreCompleted();
                        await SwapInFailover(sql);
                        return Complete();
                    case "FAILED":
                        throw new JobFailureException(response.DatabaseOperation.Error);
                    default:
                        Log.RestoreInProgress(response.DatabaseOperation.PercentComplete);
                        return Suspend(TimeSpan.FromMinutes(5), new Dictionary<string, string>()
                        {
                            {"TargetServerName", TargetServerName},
                            {"TargetDatabaseName", TargetDatabaseName},
                            {"RestoreName", RestoreName},
                            {"RestoreOperationId", RestoreOperationId}
                        });
                }
            }
        }

        private async Task SwapInFailover(SqlManagementClient sql)
        {
            // Delete the target if it exists
            Log.CheckingForExistingRestoreTarget(TargetDatabaseName);
            var existingDatabases = await sql.Databases.ListAsync(TargetServerName);
            var existingDb = existingDatabases.FirstOrDefault(db => String.Equals(db.Name, TargetDatabaseName, StringComparison.OrdinalIgnoreCase));
            string existingBackupName = null;
            if (existingDb != null)
            {
                // We need to do a swap
                Log.RestoreTargetExists();

                // Generate a name for the old restore target and rename to it
                existingBackupName = TargetDatabaseName + "_swap_" + Invocation.Id.ToString("N");
                Log.RenamingExistingRestoreTarget(TargetDatabaseName, existingBackupName);
                await sql.Databases.UpdateAsync(TargetServerName, TargetDatabaseName, new DatabaseUpdateParameters()
                {
                    CollationName = existingDb.CollationName,
                    Edition = existingDb.Edition,
                    Name = existingBackupName
                });
                Log.RenamedExistingRestoreTarget();
            }
            else
            {
                Log.RestoreTargetDoesNotExist();
            }

            ExceptionDispatchInfo error = null;
            try
            {
                // Now rename the target
                Log.RenamingNewRestoreTarget(RestoreName, TargetDatabaseName);
                var restoreDb = await sql.Databases.GetAsync(TargetServerName, RestoreName);
                await sql.Databases.UpdateAsync(TargetServerName, RestoreName, new DatabaseUpdateParameters()
                {
                    CollationName = restoreDb.Database.CollationName,
                    Edition = restoreDb.Database.Edition,
                    Name = TargetDatabaseName
                });
                Log.RenamedNewRestoreTarget();
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
                    // Recover the old export
                    Log.RecoveringExistingRestoreTarget(existingBackupName, TargetDatabaseName);
                    await sql.Databases.UpdateAsync(TargetServerName, RestoreName, new DatabaseUpdateParameters()
                    {
                        CollationName = existingDb.CollationName,
                        Edition = existingDb.Edition,
                        Name = TargetDatabaseName
                    });
                    Log.RecoveredOldRestoreTarget();
                    error.Throw();
                }
                else
                {
                    // Delete the old export
                    Log.DeletingOldRestoreTarget(existingBackupName);
                    await sql.Databases.DeleteAsync(TargetServerName, existingBackupName);
                    Log.DeletedOldRestoreTarget();
                }
            }
        }

        private static readonly Regex ServerNameMatcher = new Regex(@"(tcp:)?(?<servername>[A-Za-z0-9]*)(\.database\.windows\.net)?");
        private static string GetServerName(string fullName)
        {
            var match = ServerNameMatcher.Match(fullName);
            if (match.Success)
            {
                return match.Groups["servername"].Value;
            }
            return fullName;
        }
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-RestoreDatabaseBackup")]
    public class RestoreDatabaseBackupEventSource : EventSource
    {
        public static readonly RestoreDatabaseBackupEventSource Log = new RestoreDatabaseBackupEventSource();
        private RestoreDatabaseBackupEventSource() { }

        [Event(
            eventId: 1,
            Message = "Starting restore of {0}/{1} from {4} to {2}/{3}",
            Task = Tasks.StartingRestore,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational)]
        public void StartingRestore(string sourceServer, string sourceDb, string targetServer, string targetDb, string time) { WriteEvent(1, sourceServer, sourceDb, targetServer, targetDb, time); }

        [Event(
            eventId: 2,
            Message = "Started restore. Operation ID: {0}",
            Task = Tasks.StartingRestore,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational)]
        public void StartedRestore(string operationId) { WriteEvent(2, operationId); }

        [Event(
            eventId: 3,
            Message = "Checking status of restore {0}",
            Task = Tasks.CheckingRestoreStatus,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational)]
        public void CheckingRestoreStatus(string operationId) { WriteEvent(3, operationId); }

        [Event(
            eventId: 4,
            Message = "Restore is still in progress: {0}% complete.",
            Task = Tasks.CheckingRestoreStatus,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational)]
        public void RestoreInProgress(int percentComplete) { WriteEvent(4, percentComplete); }

        [Event(
            eventId: 5,
            Message = "Restore completed!",
            Task = Tasks.CheckingRestoreStatus,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational)]
        public void RestoreCompleted() { WriteEvent(5); }

        [Event(
            eventId: 6,
            Task = Tasks.CheckingForExistingRestoreTarget,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Checking if the restore target '{0}' exists already.")]
        public void CheckingForExistingRestoreTarget(string targetName) { WriteEvent(6, targetName); }

        [Event(
            eventId: 7,
            Task = Tasks.CheckingForExistingRestoreTarget,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Restore target exists, doing a swap.")]
        public void RestoreTargetExists() { WriteEvent(7); }

        [Event(
            eventId: 8,
            Task = Tasks.CheckingForExistingRestoreTarget,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Restore target does not exist, doing a rename.")]
        public void RestoreTargetDoesNotExist() { WriteEvent(8); }

        [Event(
            eventId: 9,
            Task = Tasks.RenamingExistingRestoreTarget,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Renaming existing restore '{0}' to '{1}'")]
        public void RenamingExistingRestoreTarget(string existingName, string newName) { WriteEvent(9, existingName, newName); }

        [Event(
            eventId: 10,
            Task = Tasks.RenamingExistingRestoreTarget,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Rename complete.")]
        public void RenamedExistingRestoreTarget() { WriteEvent(10); }

        [Event(
            eventId: 11,
            Task = Tasks.RenamingNewRestoreTarget,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Renaming restore '{0}' to '{1}'")]
        public void RenamingNewRestoreTarget(string existingName, string newName) { WriteEvent(11, existingName, newName); }

        [Event(
            eventId: 12,
            Task = Tasks.RenamingNewRestoreTarget,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Rename complete.")]
        public void RenamedNewRestoreTarget() { WriteEvent(12); }

        [Event(
            eventId: 13,
            Task = Tasks.DeletingOldRestoreTarget,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Deleting old restore target '{0}'")]
        public void DeletingOldRestoreTarget(string existingName) { WriteEvent(13, existingName); }

        [Event(
            eventId: 14,
            Task = Tasks.DeletingOldRestoreTarget,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Delete complete.")]
        public void DeletedOldRestoreTarget() { WriteEvent(14); }

        [Event(
            eventId: 15,
            Task = Tasks.RecoveringExistingRestoreTarget,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Something went wrong, restoring existing restore target '{0}' to '{1}'.")]
        public void RecoveringExistingRestoreTarget(string existingName, string newName) { WriteEvent(15, existingName, newName); }

        [Event(
            eventId: 16,
            Task = Tasks.RecoveringExistingRestoreTarget,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Recovery complete.")]
        public void RecoveredOldRestoreTarget() { WriteEvent(16); }

        public static class Tasks
        {
            public const EventTask StartingRestore = (EventTask)0x1;
            public const EventTask CheckingRestoreStatus = (EventTask)0x2;
            public const EventTask CheckingExistingRestores = (EventTask)0x3;
            public const EventTask CheckingForExistingRestoreTarget = (EventTask)0x4;
            public const EventTask RenamingExistingRestoreTarget = (EventTask)0x5;
            public const EventTask RenamingNewRestoreTarget = (EventTask)0x6;
            public const EventTask DeletingOldRestoreTarget = (EventTask)0x7;
            public const EventTask RecoveringExistingRestoreTarget = (EventTask)0x8;
        }
    }
}
