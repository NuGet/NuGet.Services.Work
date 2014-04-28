using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
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

        protected AzureHub Azure { get; set; }
        protected ConfigurationHub Config { get; set; }

        public RestoreDatabaseBackupJob(AzureHub azure)
        {
            Azure = azure;
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

            using (var sql = CloudContext.Clients.CreateSqlManagementClient(Azure.GetCredentials()))
            {
                var response = await sql.RestoreDatabaseOperations.CreateAsync(
                    SourceServerName, new RestoreDatabaseOperationCreateParameters()
                    {
                        PointInTime = backupTime,
                        SourceDatabaseName = SourceDatabaseName,
                        TargetDatabaseName = TargetDatabaseName,
                        TargetServerName = TargetServerName
                    });
                
                // Would have thrown if it failed, so now start checking the status
                RestoreOperationId = response.Operation.Id;
                return await Resume();
            }
        }

        protected internal override async Task<JobContinuation> Resume()
        {
            // Check the status of the operation
            using (var sql = CloudContext.Clients.CreateSqlManagementClient(Azure.GetCredentials()))
            {
                var response = await sql.DatabaseOperations.GetAsync(TargetServerName, RestoreOperationId);
                switch(response.DatabaseOperation.State) {
                    case "COMPLETED":
                        return Complete();
                    case "FAILED":
                        // Barf!
                        throw new JobFailureException()

                }
            }

            return Suspend(TimeSpan.FromMinutes(5), new Dictionary<string, string>()
            {
                {"TargetServerName", TargetServerName},
                {"RestoreOperationId", RestoreOperationId}
            });
            return base.Resume();
        }
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-RestoreDatabaseBackup")]
    public class RestoreDatabaseBackupEventSource : EventSource
    {
        public static readonly RestoreDatabaseBackupEventSource Log = new RestoreDatabaseBackupEventSource();
        private RestoreDatabaseBackupEventSource() { }

        public static class Task
        {
            Fill this in next!
        }
    }
}
