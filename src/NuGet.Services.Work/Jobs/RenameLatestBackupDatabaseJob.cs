using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Jobs.Models;

namespace NuGet.Services.Work.Jobs
{
    [Description("Renames latest backup database to gallery database name")]
    public class RenameLatestBackupDatabaseJob : DatabaseJobHandlerBase<RenameLatestBackupDatabaseEventSource>
    {
        private const string RenameDatabase = @"ALTER DATABASE [{0}] MODIFY NAME = [{1}]";
        private const string DefaultGalleryDBName = "NuGetGallery";
        private const string TempBackupName = "TempBackup";

        /// <summary>
        /// The prefix to search for the backup
        /// </summary>
        public string BackupPrefix { get; set; }

        public string GalleryDBName { get; set; }

        public RenameLatestBackupDatabaseJob(ConfigurationHub configHub) : base(configHub) { }
        protected internal override async Task<JobContinuation> Execute()
        {
            var cstr = GetConnectionString() ?? Config.Sql.GetConnectionString(KnownSqlConnection.Primary);
            if (cstr == null || cstr.InitialCatalog == null || cstr.Password == null || cstr.DataSource == null || cstr.UserID == null)
            {
                throw new ArgumentNullException("One of the connection string parameters or the string itself is null");
            }

            cstr.TrimNetworkProtocol();
            Log.PreparingToRename(cstr.DataSource); // EventId: 1


            if (String.IsNullOrEmpty(GalleryDBName))
            {
                GalleryDBName = DefaultGalleryDBName;
            }
            Log.GalleryDBName(GalleryDBName); // EventId: 2

            if (String.IsNullOrEmpty(BackupPrefix))
            {
                BackupPrefix = CreateOnlineDatabaseBackupJob.DefaultBackupPrefix;
            }
            Log.BackupPrefix(BackupPrefix); // EventId: 3

            using (SqlConnection connection = await cstr.ConnectToMaster())
            {
                Log.ConnectedToMaster();  // EventId: 4

                var backupDatabase = await GetLatestOnlineBackupDatabase(connection);
                if (backupDatabase == null)
                {
                    throw new ArgumentException("Backup Database not found");
                }

                var backupName = backupDatabase.name;
                Log.BackupName(backupName);  // EventId: 5

                var galleryDatabase = await GetDatabase(connection, GalleryDBName);
                if (galleryDatabase == null)
                {
                    Log.GalleryDatabaseNotFound(GalleryDBName);  // EventId: 6
                    // NO Gallery Database was found. This is BAD
                    // Simply rename latest backup database to GalleryDatabase and bail
                    await connection.ExecuteAsync(String.Format(RenameDatabase, backupName, GalleryDBName));
                    return Complete();
                }

                // If Gallery Database was found and latest backup was available
                // Check if GalleryDatabase is newer than latest backup
                // If so, ignore. Otherwise, Rename
                if (backupDatabase.create_date > galleryDatabase.create_date)
                {
                    Log.RenameNeeded();  // EventId: 7
                    Log.RenamingBackupToTemp(backupName, TempBackupName); // EventId: 8
                    await connection.ExecuteAsync(String.Format(RenameDatabase, backupName, TempBackupName));
                    Log.RenamedBackupToTemp(); // EventId: 9
                    Log.RenamingNuGetGalleryToBackup(GalleryDBName, backupName); // EventId: 10
                    await connection.ExecuteAsync(String.Format(RenameDatabase, GalleryDBName, backupName));
                    Log.RenamedNuGetGalleryToBackup(); // EventId: 11
                    Log.RenamingTempToNuGetGallery(TempBackupName, GalleryDBName); // EventId: 12
                    await connection.ExecuteAsync(String.Format(RenameDatabase, TempBackupName, GalleryDBName));
                    Log.RenamedTempToNuGetGallery(); // EventId: 13
                }
                else
                {
                    Log.NoRenameNeeded(); // EventId: 14
                }
            }

            return Complete();
        }

        private async Task<Database> GetLatestOnlineBackupDatabase(SqlConnection connection)
        {
            // Get databases
            var databases = await GetDatabases(connection, DatabaseState.ONLINE);

            // Gather backups with matching prefix and order descending
            var ordered = from db in databases
                          let backupMeta = db.GetBackupMetadata()
                          where backupMeta != null &&
                              String.Equals(
                                  BackupPrefix,
                                  backupMeta.Prefix,
                                  StringComparison.OrdinalIgnoreCase)
                          orderby backupMeta.Timestamp descending
                          select backupMeta;

            // Take the most recent one and check it's time
            var mostRecent = ordered.FirstOrDefault();
            if (mostRecent != null)
            {
                return mostRecent.Db;
            }

            return null;
        }
    }

    public class RenameLatestBackupDatabaseEventSource : EventSource
    {
        public static readonly RenameLatestBackupDatabaseEventSource Log = new RenameLatestBackupDatabaseEventSource();

        private RenameLatestBackupDatabaseEventSource() { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Preparing to rename databases on '{0}'")]
        public void PreparingToRename(string server) { WriteEvent(1, server); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Message = "Gallery database name is '{0}'")]
        public void GalleryDBName(string galleryDBName) { WriteEvent(2, galleryDBName); }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Message = "Backup prefix is '{0}'")]
        public void BackupPrefix(string backupPrefix) { WriteEvent(3, backupPrefix); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Message = "Connected to master")]
        public void ConnectedToMaster() { WriteEvent(4); }

        [Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Message = "Backup Name is '{0}'")]
        public void BackupName(string backupName) { WriteEvent(5, backupName); }

        [Event(
            eventId: 6,
            Level = EventLevel.Informational,
            Message = "Gallery Database '{0}' not found")]
        public void GalleryDatabaseNotFound(string galleryDBName) { WriteEvent(6, galleryDBName); }

        [Event(
            eventId: 7,
            Level = EventLevel.Informational,
            Message = "Rename Needed")]
        public void RenameNeeded() { WriteEvent(7); }

        [Event(
            eventId: 8,
            Level = EventLevel.Informational,
            Message = "Renaming backup database '{0}' to temp database '{1}'")]
        public void RenamingBackupToTemp(string backupName, string tempName) { WriteEvent(8, backupName, tempName); }

        [Event(
            eventId: 9,
            Level = EventLevel.Informational,
            Message = "Renamed backup to Temp")]
        public void RenamedBackupToTemp() { WriteEvent(9); }

        [Event(
            eventId: 10,
            Level = EventLevel.Informational,
            Message = "Renaming nuget gallery database '{0}' to backup database '{1}'")]
        public void RenamingNuGetGalleryToBackup(string galleryDBName, string backupName) { WriteEvent(10, galleryDBName, backupName); }

        [Event(
            eventId: 11,
            Level = EventLevel.Informational,
            Message = "Renamed nuget gallery to backup")]
        public void RenamedNuGetGalleryToBackup() { WriteEvent(11); }

        [Event(
            eventId: 12,
            Level = EventLevel.Informational,
            Message = "Renaming temp database '{0}' to nuget gallery database '{1}'")]
        public void RenamingTempToNuGetGallery(string tempName, string galleryDBName) { WriteEvent(12, tempName, galleryDBName); }

        [Event(
            eventId: 13,
            Level = EventLevel.Informational,
            Message = "RenamedTempToNuGetGallery")]
        public void RenamedTempToNuGetGallery() { WriteEvent(13); }

        [Event(
            eventId: 14,
            Level = EventLevel.Informational,
            Message = "No rename needed")]
        public void NoRenameNeeded() { WriteEvent(14); }
    }
}
