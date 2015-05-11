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
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Jobs.Models;

namespace NuGet.Services.Work.Jobs
{
    [Description("Imports a bacpac file into database")]
    public class ImportDatabaseJob : DatabaseJobHandlerBase<ImportDatabaseEventSource>
    {
        public static readonly string DefaultBackupPrefix = "backup";
        private const string RenameDatabase = @"ALTER DATABASE [{0}] MODIFY NAME = [{1}]";
        private const string DropDatabase = @"DROP DATABASE [{0}]";
        private const string DefaultGalleryDBName = "NuGetGallery";
        private const string TempBackupName = "TempBackup";
        public string SourceStorageAccountName { get; set; }

        public string SourceStorageAccountKey { get; set; }

        public string BacpacFile { get; set; }

        public string RequestGUID { get; set; }

        public string EndPointUri { get; set; }

        public string GalleryDBName { get; set; }

        public int RenameAttempts { get; set; }

        public string BackupPrefix { get; set; }

        public ImportDatabaseJob(ConfigurationHub configHub) : base(configHub) { }

        protected internal override async Task<JobContinuation> Execute()
        {
            // Load Defaults
            var endPointUri = EndPointUri ?? Config.Sql.ImportEndPoint;
            if (String.IsNullOrEmpty(endPointUri))
            {
                endPointUri = NuGet.Services.Constants.EastUSEndpoint;
            }

            Log.ImportEndpoint(endPointUri);

            var cstr = GetConnectionString() ?? Config.Sql.GetConnectionString(KnownSqlConnection.Primary);
            if (cstr == null || cstr.InitialCatalog == null || cstr.Password == null || cstr.DataSource == null || cstr.UserID == null)
            {
                throw new ArgumentNullException("One of the connection string parameters or the string itself is null");
            }

            cstr.TrimNetworkProtocol();
            Log.PreparingToImport(cstr.ToString());

            if (String.IsNullOrEmpty(GalleryDBName))
            {
                GalleryDBName = DefaultGalleryDBName;
            }
            Log.GalleryDBName(GalleryDBName);

            if (String.IsNullOrEmpty(BackupPrefix))
            {
                BackupPrefix = DefaultBackupPrefix;
            }

            if (SourceStorageAccountName == null && SourceStorageAccountKey == null)
            {
                var sourceCredentials = Config.Storage.Primary.Credentials;
                SourceStorageAccountName = sourceCredentials.AccountName;
                SourceStorageAccountKey = sourceCredentials.ExportBase64EncodedKey();
            }
            else
            {
                if (SourceStorageAccountName == null)
                {
                    throw new ArgumentNullException("Source Storage Account Name is null");
                }

                if (SourceStorageAccountKey == null)
                {
                    throw new ArgumentNullException("Source Storage Account Key is null");
                }
            }

            if (String.IsNullOrEmpty(BacpacFile))
            {
                var storageCredentials = new StorageCredentials(SourceStorageAccountName, SourceStorageAccountKey);
                var blobEndPoint = String.Format(@"https://{0}.blob.core.windows.net", SourceStorageAccountName);
                var cloudBlobClient = new CloudBlobClient(new Uri(blobEndPoint), storageCredentials);

                BacpacFile = GetLatestBackupBacpacFile(cloudBlobClient);
            }

            var dotIndex = BacpacFile.IndexOf('.');
            BacpacFile = dotIndex > -1 ? BacpacFile.Substring(0, dotIndex) : BacpacFile;

            TargetDatabaseName = BacpacFile;

            if (await DoesDBExist(cstr, TargetDatabaseName))
            {
                Log.DatabaseAlreadyExists("Database {0} already exists.Skipping...", TargetDatabaseName);

                // If we reached this point, certainly, the import is incomplete/failed
                // Because, if RENAME failed after a successful import, we keep trying again until rename is successful
                // If import failed, we do not RENAME at all. So, DO NOT RENAME HERE
                return Complete();
            }

            WASDImportExport.ImportExportHelper helper = new WASDImportExport.ImportExportHelper()
            {
                EndPointUri = endPointUri,
                DatabaseName = TargetDatabaseName,
                ServerName = cstr.DataSource,
                UserName = cstr.UserID,
                Password = cstr.Password,
                StorageKey = SourceStorageAccountKey,
            };

            var blobAbsoluteUri = String.Format(@"https://{0}.blob.core.windows.net/bacpac-files/{1}.bacpac", SourceStorageAccountName, BacpacFile);

            Log.StartingImport(TargetDatabaseName, cstr.DataSource, BacpacFile);

            var requestGUID = helper.DoImport(Log, blobAbsoluteUri, whatIf: WhatIf);

            if (requestGUID != null)
            {
                Log.ImportStarted(requestGUID);

                var parameters = new Dictionary<string, string>();
                parameters["RequestGUID"] = requestGUID;
                parameters["TargetDatabaseConnection"] = cstr.ConnectionString;
                parameters["EndPointUri"] = endPointUri;
                parameters["TargetDatabaseName"] = TargetDatabaseName;
                parameters["GalleryDBName"] = GalleryDBName;

                return Suspend(TimeSpan.FromMinutes(5), parameters);
            }
            else
            {
                throw new Exception("Request to import database unsuccessful. No request GUID obtained");
            }
        }

        protected internal override async Task<JobContinuation> Resume()
        {
            if (RenameAttempts > 0)
            {
                if (TargetDatabaseConnection == null || TargetDatabaseName == null || String.IsNullOrEmpty(GalleryDBName))
                {
                    throw new ArgumentNullException("Job could not resume properly due to incorrect parameters");
                }
                return await RenameImportedDBToGalleryDB();
            }
            else
            {
                if (RequestGUID == null || TargetDatabaseConnection == null || EndPointUri == null || TargetDatabaseName == null || String.IsNullOrEmpty(GalleryDBName))
                {
                    throw new ArgumentNullException("Job could not resume properly due to incorrect parameters");
                }

                var endPointUri = EndPointUri;
                WASDImportExport.ImportExportHelper helper = new WASDImportExport.ImportExportHelper()
                {
                    EndPointUri = endPointUri,
                    ServerName = TargetDatabaseConnection.DataSource,
                    UserName = TargetDatabaseConnection.UserID,
                    Password = TargetDatabaseConnection.Password,
                    DatabaseName = TargetDatabaseName,
                };

                DACWebService.StatusInfo statusInfo = null;

                try
                {
                    var statusInfoList = helper.CheckRequestStatus(RequestGUID);
                    statusInfo = statusInfoList.FirstOrDefault();
                }
                catch (Exception ex)
                {
                    Log.Exception(ex.Message);
                }

                if (statusInfo != null)
                {
                    if (statusInfo.Status == "Failed")
                    {
                        Log.ImportFailed(statusInfo.ErrorMessage);
                        throw new Exception(statusInfo.ErrorMessage);
                    }

                    if (statusInfo.Status == "Completed")
                    {
                        Log.ImportCompleted(statusInfo.DatabaseName, helper.ServerName);

                        // Now, that the import is complete, rename DB
                        RenameAttempts = 1;
                        return await RenameImportedDBToGalleryDB();
                    }

                    Log.Importing(statusInfo.Status);
                }

                var parameters = new Dictionary<string, string>();
                parameters["RequestGUID"] = RequestGUID;
                parameters["TargetDatabaseConnection"] = TargetDatabaseConnection.ConnectionString;
                parameters["EndPointUri"] = endPointUri;
                parameters["TargetDatabaseName"] = TargetDatabaseName;
                parameters["GalleryDBName"] = GalleryDBName;
                return Suspend(TimeSpan.FromMinutes(5), parameters);
            }
        }

        private string GetLatestBackupBacpacFile(CloudBlobClient cloudBlobClient)
        {
            try
            {
                // Get a reference to bacpac files container
                var bacpacFileContainer = cloudBlobClient.GetContainerReference(BlobContainerNames.BacpacFiles);

                var blobItems = bacpacFileContainer.ListBlobs(BackupPrefix, useFlatBlobListing: true);
                if (blobItems == null)
                {
                    throw new Exception("No blobs found in bacpacfiles container. That is a mystery!");
                }

                var cloudBlobs = from blobItem in blobItems
                                 where (blobItem as ICloudBlob) != null
                                 select (blobItem as ICloudBlob);

                var latestbacpacFile = (from cloudBlob in cloudBlobs
                                       orderby cloudBlob.Properties.LastModified descending select cloudBlob.Name).FirstOrDefault();
                if (String.IsNullOrEmpty(latestbacpacFile))
                {
                    throw new Exception("No bacpac file with a prefix Backup could be found");
                }

                return latestbacpacFile;
            }
            catch (StorageException storageEx)
            {
                Exception ex = new Exception("Could not obtain a backup bacpac file", storageEx);
                throw ex;
            }
        }

        private async Task<bool> DoesDBExist(SqlConnectionStringBuilder cstr, string name)
        {
            // Connect to the master database
            using (var connection = await cstr.ConnectToMaster())
            {
                var db = await GetDatabase(connection, name);
                return db != null;
            }
        }

        private async Task<JobContinuation> RenameImportedDBToGalleryDB()
        {
            var cstr = TargetDatabaseConnection;
            if (cstr == null || cstr.InitialCatalog == null || cstr.Password == null || cstr.DataSource == null || cstr.UserID == null)
            {
                throw new ArgumentNullException("One of the connection string parameters or the string itself is null");
            }

            cstr.TrimNetworkProtocol();
            Log.PreparingToRename(cstr.DataSource, RenameAttempts);

            Log.GalleryDBName(GalleryDBName);

            try
            {
                using (SqlConnection connection = await cstr.ConnectToMaster())
                {
                    Log.ConnectedToMaster();

                    var backupDatabase = await GetDatabase(connection, TargetDatabaseName);
                    if (backupDatabase == null)
                    {
                        throw new ArgumentException("Backup Database not found");
                    }

                    var backupName = backupDatabase.name;
                    Log.BackupName(backupName);

                    var galleryDatabase = await GetDatabase(connection, GalleryDBName);
                    if (galleryDatabase == null)
                    {
                        Log.GalleryDatabaseNotFound(GalleryDBName);
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
                        Log.RenameNeeded();

                        var tempBackupDatabase = await GetDatabase(connection, TempBackupName);
                        if (tempBackupDatabase != null)
                        {
                            Log.DroppingExistingTempBackup(tempBackupDatabase.ToString());
                            await connection.ExecuteAsync(String.Format(DropDatabase, tempBackupDatabase.name));
                            Log.DroppedTempBackup();
                        }

                        Log.RenamingBackupToTemp(backupName, TempBackupName);
                        await connection.ExecuteAsync(String.Format(RenameDatabase, backupName, TempBackupName));
                        Log.RenamedBackupToTemp();
                        Log.RenamingNuGetGalleryToBackup(GalleryDBName, backupName);
                        await connection.ExecuteAsync(String.Format(RenameDatabase, GalleryDBName, backupName));
                        Log.RenamedNuGetGalleryToBackup();
                        Log.RenamingTempToNuGetGallery(TempBackupName, GalleryDBName);
                        await connection.ExecuteAsync(String.Format(RenameDatabase, TempBackupName, GalleryDBName));
                        Log.RenamedTempToNuGetGallery();
                    }
                    else
                    {
                        Log.NoRenameNeeded();
                    }
                }
                return Complete();
            }
            catch (SqlException ex)
            {
                Log.Exception(ex.Message);
            }

            // While import has already completed successfully, Rename failed for some reason
            // We will try again later if RenameAttempts is less than 5
            if (RenameAttempts < 5)
            {
                var parameters = new Dictionary<string, string>();
                parameters["RenameAttempts"] = (RenameAttempts + 1).ToString();
                parameters["TargetDatabaseConnection"] = TargetDatabaseConnection.ConnectionString;
                parameters["TargetDatabaseName"] = TargetDatabaseName;
                parameters["GalleryDBName"] = GalleryDBName;

                return Suspend(TimeSpan.FromMinutes(3), parameters);
            }

            // Else, We throw and fail
            throw new Exception("Rename DB did not succeed for 5 attempts");
        }
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-ImportDatabase")]
    public class ImportDatabaseEventSource : EventSource
    {
        public static readonly ImportDatabaseEventSource Log = new ImportDatabaseEventSource();

        private ImportDatabaseEventSource() { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Import endpoint is {0}")]
        public void ImportEndpoint(string endPointUri) { WriteEvent(1, endPointUri); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Message = "Preparing to import to {0}")]
        public void PreparingToImport(string cstr) { WriteEvent(2, cstr); }

        [Event(
            eventId: 3,
            Level = EventLevel.Warning,
            Message = "Target database {0} already exists on {1}!")]
        public void DatabaseAlreadyExists(string database, string server) { WriteEvent(3, database, server); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Message = "Starting Import of database {0} on server {1} from bacpac file {2}")]
        public void StartingImport(string database, string server, string bacpacFile) { WriteEvent(4, database, server, bacpacFile); }

        [Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Message = "Import has started successfully. Request GUID: {0}")]
        public void ImportStarted(string requestGUID) { WriteEvent(5, requestGUID); }

        [Event(
            eventId: 6,
            Level = EventLevel.Error,
            Message = "Import operation failed. Error Message: {0}")]
        public void ImportFailed(string message) { WriteEvent(6, message); }

        [Event(
            eventId: 7,
            Level = EventLevel.Informational,
            Message = "Import has completed successfully. Database has been imported to {0} on server {1}")]
        public void ImportCompleted(string database, string server) { WriteEvent(7, database, server); }

        [Event(
            eventId: 8,
            Level = EventLevel.Informational,
            Message = "Import is still in progress. Status : {0}")]
        public void Importing(string statusMessage) { WriteEvent(8, statusMessage); }

        [Event(
            eventId: 9,
            Level = EventLevel.Informational,
            Message = "HTTP Posting to requestURI : {0}")]
        public void RequestUri(string requestUri) { WriteEvent(9, requestUri); }

        [Event(
            eventId: 10,
            Level = EventLevel.Informational,
            Message = "Would have sent : {0}")]
        public void WouldHaveSent(string request) { WriteEvent(10, request); }

        [Event(
            eventId: 11,
            Level = EventLevel.Informational,
            Message = "Sending request : {0}")]
        public void SendingRequest(string request) { WriteEvent(11, request); }

        [Event(
            eventId: 12,
            Level = EventLevel.Informational,
            Message = "Request failed. Exception message : {0}")]
        public void RequestFailed(string exceptionMessage) { WriteEvent(12, exceptionMessage); }

        [Event(
            eventId: 13,
            Level = EventLevel.Informational,
            Message = "HttpWebResponse error code. Status : {0}")]
        public void ErrorStatusCode(int statusCode) { WriteEvent(13, statusCode); }

        [Event(
            eventId: 14,
            Level = EventLevel.Informational,
            Message = "HttpWebResponse error description. Status : {0}")]
        public void ErrorStatusDescription(string statusDescription) { WriteEvent(14, statusDescription); }

        [Event(
            eventId: 21,
            Level = EventLevel.Informational,
            Message = "Preparing to rename databases on '{0}'. Attempt : {1}")]
        public void PreparingToRename(string server, int renameAttempts) { WriteEvent(21, server, renameAttempts); }

        [Event(
            eventId: 22,
            Level = EventLevel.Informational,
            Message = "Gallery database name is '{0}'")]
        public void GalleryDBName(string galleryDBName) { WriteEvent(22, galleryDBName); }

        [Event(
            eventId: 23,
            Level = EventLevel.Informational,
            Message = "Backup prefix is '{0}'")]
        public void BackupPrefix(string backupPrefix) { WriteEvent(23, backupPrefix); }

        [Event(
            eventId: 24,
            Level = EventLevel.Informational,
            Message = "Connected to master")]
        public void ConnectedToMaster() { WriteEvent(24); }

        [Event(
            eventId: 25,
            Level = EventLevel.Informational,
            Message = "Backup Name is '{0}'")]
        public void BackupName(string backupName) { WriteEvent(25, backupName); }

        [Event(
            eventId: 26,
            Level = EventLevel.Informational,
            Message = "Gallery Database '{0}' not found")]
        public void GalleryDatabaseNotFound(string galleryDBName) { WriteEvent(26, galleryDBName); }

        [Event(
            eventId: 27,
            Level = EventLevel.Informational,
            Message = "Rename Needed")]
        public void RenameNeeded() { WriteEvent(27); }

        [Event(
            eventId: 28,
            Level = EventLevel.Informational,
            Message = "Renaming backup database '{0}' to temp database '{1}'")]
        public void RenamingBackupToTemp(string backupName, string tempName) { WriteEvent(28, backupName, tempName); }

        [Event(
            eventId: 29,
            Level = EventLevel.Informational,
            Message = "Renamed backup to Temp")]
        public void RenamedBackupToTemp() { WriteEvent(29); }

        [Event(
            eventId: 30,
            Level = EventLevel.Informational,
            Message = "Renaming nuget gallery database '{0}' to backup database '{1}'")]
        public void RenamingNuGetGalleryToBackup(string galleryDBName, string backupName) { WriteEvent(30, galleryDBName, backupName); }

        [Event(
            eventId: 31,
            Level = EventLevel.Informational,
            Message = "Renamed nuget gallery to backup")]
        public void RenamedNuGetGalleryToBackup() { WriteEvent(31); }

        [Event(
            eventId: 32,
            Level = EventLevel.Informational,
            Message = "Renaming temp database '{0}' to nuget gallery database '{1}'")]
        public void RenamingTempToNuGetGallery(string tempName, string galleryDBName) { WriteEvent(32, tempName, galleryDBName); }

        [Event(
            eventId: 33,
            Level = EventLevel.Informational,
            Message = "RenamedTempToNuGetGallery")]
        public void RenamedTempToNuGetGallery() { WriteEvent(33); }

        [Event(
            eventId: 34,
            Level = EventLevel.Informational,
            Message = "No rename needed")]
        public void NoRenameNeeded() { WriteEvent(34); }

        [Event(
            eventId: 35,
            Level = EventLevel.Informational,
            Message = "Temp Backup database already exists. Dropping it. TempBackupDatabase --> {0}")]
        public void DroppingExistingTempBackup(string databaseInfo) { WriteEvent(35, databaseInfo); }

        [Event(
            eventId: 36,
            Level = EventLevel.Informational,
            Message = "Dropped Temp Backup database")]
        public void DroppedTempBackup() { WriteEvent(36); }

        [Event(
            eventId: 37,
            Level = EventLevel.Informational,
            Message = "Exception Caught. Message: {0}. Moving on...")]
        public void Exception(string message) { WriteEvent(37, message); }
    }
}
