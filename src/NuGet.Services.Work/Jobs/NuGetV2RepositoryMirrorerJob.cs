// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
extern alias NuGetCoreRef;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json.Linq;
using NuGet;
using NuGet.Services.Configuration;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.Services.Client;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using DataServices = NuGetCoreRef::System.Data.Services.Client;

namespace NuGet.Services.Work.Jobs
{
    public class DataServicePackageWithCreated : DataServicePackage
    {
        public static readonly DateTime UnlistedPublishedUtc = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public DateTimeOffset? Created { get; set; }
        public SemanticVersion SemanticVersion
        {
            get
            {
                return (this as IPackage).Version;
            }
        }
        public bool IsListed
        {
            get
            {
                return !Published.Value.DateTime.Equals(UnlistedPublishedUtc);
            }
        }
    }
    /// <summary>
    /// SourceExceptions are thrown when installing a package locally from Source fails
    /// </summary>
    class SourceException : Exception
    {
        private const string SourceExceptionMessage = "Source Exception: ";
        public SourceException(Exception innerException) : base(SourceExceptionMessage + innerException.Message, innerException) { }
    }

    /// <summary>
    /// DestinationExceptions are thrown when pushing a package to the destination fails
    /// </summary>
    class DestinationException : Exception
    {
        private const string DestinationExceptionMessage = "Destination Exception: ";
        public DestinationException(Exception innerException) : base(DestinationExceptionMessage + innerException.Message, innerException) { }
    }

    [Description("Job to mirror a NuGet V2 Repository by polling on a defined interval")]
    public class NuGetV2RepositoryMirrorerJob : JobHandler<NuGetV2RepositoryMirrorerEventSource>
    {
        // Mirror Json Keys
        public const string LastCreatedKey = "lastCreated";
        public const string IdKey = "id";
        public const string VersionKey = "version";
        public const string SourceCreatedKey = "sourceCreated";
        public const string DeletedKey = "deleted";
        public const string ListedKey = "listed";
        public const string PackageIndexKey = "packageIndex";

        private const string ContentTypeJson = "application/json";
        private const string DateTimeFormatSpecifier = "O";
        private const int PushTimeOutInMinutes = 5;
        private const int MaxRetries = 5;
        private const string DefaultMirrorBlobContainerName = "mirror";
        private const string DefaultMirrorBlobName = "mirror.json";

        public string SourceV2Feed { get; set; }
        public string DestinationUri { get; set; }
        public string ApiKey { get; set; }
        public CloudStorageAccount MirrorStorage { get; set; }
        public string MirrorBlobContainerName { get; set; }
        public string MirrorBlobName { get; set; }
        public bool ExecuteDeletes { get; set; }

        protected ConfigurationHub Config { get; private set; }

        private CloudBlobContainer MirrorBlobContainer { get; set; }
        private string UserAgent { get; set; }

        public NuGetV2RepositoryMirrorerJob(ConfigurationHub config)
        {
            Config = config;
            AddEventSource(NuGetV2RepositoryMirrorPackageDeletorEventSource.Log);
        }

        protected async internal override Task Execute()
        {
            // Validate mandatory parameters
            if (String.IsNullOrEmpty(SourceV2Feed))
            {
                throw new ArgumentException("SourceV2Feed cannot be null or empty");
            }

            // If the SourceV2Feed is not a URI, the following line will throw URIFormatException
            var sourceV2FeedUri = new Uri(SourceV2Feed);

            if (String.IsNullOrEmpty(DestinationUri))
            {
                throw new ArgumentException("DestinationUri cannot be null or empty");
            }

            if (String.IsNullOrEmpty(ApiKey))
            {
                throw new ArgumentException("ApiKey cannot be null or empty");
            }

            // Packages are in Legacy account. PackageDatabase is the InitialCatalog in the legacy account
            var account = Config.Storage.Legacy;
            var cstr = Config.Sql.Legacy;
            if (cstr == null)
            {
                throw new ArgumentNullException("Legacy sql cannot be null");
            }

            // Arrange or set defaults for parameters that are not provided
            UserAgent = String.Format("{0}v2FeedMirrorer", sourceV2FeedUri.DnsSafeHost);
            var timeOutPerPush = TimeSpan.FromMinutes(PushTimeOutInMinutes);
            MirrorStorage = MirrorStorage ?? Config.Storage.Legacy;

            if (MirrorStorage == null)
            {
                throw new ArgumentNullException("MirrorStorage", "Mirror storage is not provided or present in the config");
            }

            MirrorBlobContainerName = String.IsNullOrEmpty(MirrorBlobContainerName) ? DefaultMirrorBlobContainerName : MirrorBlobContainerName;
            MirrorBlobContainer = MirrorStorage.CreateCloudBlobClient().GetContainerReference(MirrorBlobContainerName);
            MirrorBlobName = String.IsNullOrEmpty(MirrorBlobName) ? DefaultMirrorBlobName : MirrorBlobName;

            var sourceUri = new Uri(sourceV2FeedUri, "/api/v2/");
            var serviceContext = new DataServices.DataServiceContext(sourceUri)
            {
                MergeOption = DataServices.MergeOption.OverwriteChanges,
                IgnoreMissingProperties = true,
            };

            var mirrorJson = await GetJObject(MirrorBlobContainer, MirrorBlobName);
            if (!IsMirrorJsonValid(mirrorJson))
            {
                throw new InvalidOperationException("mirrorJson is not valid. Either packageIndex array is not present. Or, the elements are not sorted by SourceCreatedDate");
            }

            Exception caughtException = null;

            if (ExecuteDeletes)
            {
                try
                {
                    await NuGetV2RepositoryMirrorPackageDeletor.DeleteAndSetListedPackages(sourceUri, mirrorJson, account, cstr);
                }
                catch (Exception ex)
                {
                    caughtException = ex;
                }
                await SetJObject(MirrorBlobContainer, MirrorBlobName, mirrorJson);
                if (caughtException != null)
                {
                    throw caughtException;
                }
                return;
            }

            var oldLastCreated = mirrorJson.Value<DateTime>(LastCreatedKey);
            var lastCreated = oldLastCreated;
            Log.PreparingToMirror(MirrorBlobName, lastCreated.ToString(DateTimeFormatSpecifier), lastCreated.Kind.ToString(), sourceUri.AbsoluteUri, DestinationUri);

            int retries = 0;
            int count = 0;
            int skipIndex = 0;

            //
            //  POSSIBLE ACTIONS when an error is encountered
            //  A) Always Skip to next package
            //  B) Retry 'MaxRetries' times and skip to next package
            //  C) Retry 'MaxRetries' times and fail
            //
            //	KNOWN ERRORS
            //	1) '409 Conflict' from Destination- Package already exists in destination
            //		i)  Action: (A). Always Skip to next package
            //      ii) Before skipping, Update lastMirroredPackage.Created locally, i.e inside the while loop
            //	2) '403 Forbidden' from Source. For reasons unknown, certain listed packages are not available for download. Source returns "Access Denied"
            //      i)  Action: (B). Retry 'MaxRetries' times and Skip to next package
            //      ii) Log every retry. Before skipping, Update lastMirroredPackage.Created locally, i.e inside the while loop
            //  3) '404 Not Found' from Source. Package is available on the feed but not available for download already
            //      i)  Action: (C). Retry 'MaxRetries' times and Fail
            //      ii) Log every retry. Update lastMirroredPackage.Created in blob storage
            //	4) Unknown Error
            //      i)  Action: (C). Retry 'MaxRetries' times and Fail
            //      ii) Log every retry. Update lastMirroredPackage.Created in blob storage
            //
            // Test Cases
            // 1) Add a new package to the source. COVERED
            //	  Result: The new package should be present on the destination
            // 2) Add a package as unlisted to the source. COVERED
            //	  Result: The new package should be present on the destination as unlisted
            // 3) Delete a package version from the source. COVERED
            //	  Result: The package should be deleted from the destination
            // 4) Delete a package version from the source which is the last version with the package Id. NOT COVERED
            //	  Result: The package should be deleted from the destination. And, PackageRegistration should be deleted too as appropriate
            // 5) Delete a package version from the source. And, add a new package with same Id and version as the deleted one. COVERED
            //	  Result: Old package with Id and version must be deleted from the destination. And, the new package with the same Id and Version must be added
            // 6) Mark a listed package as unlisted. COVERED
            //	  Result: The package should be unlisted in the destination too
            // 7) Mark an unlisted package as listed. COVERED
            //	  Result: The package should be listed in the destination too
            //
            try
            {
                do
                {
                    if ((Invocation.NextVisibleAt - DateTimeOffset.UtcNow) < TimeSpan.FromMinutes(120))
                    {
                        // Based on default configuration on the repository, there are at most 40 packages returned by a single query (which will get downloaded and pushed)
                        // With a conservative estimate of 3 minutes per package, expecting a minimum of
                        // 120 minutes to run 1 iteration of this do-while loop
                        await Extend(TimeSpan.FromMinutes(120));
                    }

                    // In each query, at most, 40 packages may be returned. So, continue performing the queries
                    // in this do-while, so long as there are results returned

                    // Query for packages created since oldLastCreated
                    PackageServer destinationServer = new PackageServer(DestinationUri, UserAgent);

                    var lastMirroredPackage = QueryAndMirrorBatch(serviceContext, destinationServer, oldLastCreated, ApiKey, timeOutPerPush.Milliseconds, mirrorJson, ref retries, ref count, ref skipIndex, cstr, account);
                    if (lastMirroredPackage != null)
                    {
                        if (!lastMirroredPackage.Created.HasValue)
                        {
                            throw new InvalidOperationException("Last mirrored package : " + lastMirroredPackage.ToString() + "has a null Created Time.. WRONG!!!");
                        }

                        // Note that the Created DateTime is always stored in UTC, but the DateTimeKind of the value obtained is Unspecified
                        // So, store it as UTC
                        lastCreated = new DateTime(lastMirroredPackage.Created.Value.DateTime.Ticks, DateTimeKind.Utc);
                        Log.EndOfIteration(lastCreated.ToString(DateTimeFormatSpecifier));
                    }
                    else if (retries == 0 || retries > MaxRetries)
                    {
                        break;
                    }
                } while (true);
            }
            catch (Exception ex)
            {
                // Catch the exception here so that new lastCreated is always stored
                // We can throw this exception at the end
                caughtException = ex;
            }

            if (!oldLastCreated.Equals(lastCreated))
            {
                mirrorJson[LastCreatedKey] = lastCreated.ToString(DateTimeFormatSpecifier);
                await SetJObject(MirrorBlobContainer, MirrorBlobName, mirrorJson);
                Log.NewLastCreatedAtEndOfInvocation(MirrorBlobName, lastCreated.ToString(DateTimeFormatSpecifier), lastCreated.Kind.ToString());
            }

            if (caughtException != null)
            {
                throw caughtException;
            }
        }

        private DataServices.DataServiceQuery<DataServicePackageWithCreated> GetNewPackagesToMirror(DataServices.DataServiceContext serviceContext, DateTime lastCreated)
        {
            Log.QueryNewPackages(serviceContext.BaseUri.AbsoluteUri, lastCreated.ToString(DateTimeFormatSpecifier));
            var packagesQuery = serviceContext.CreateQuery<DataServicePackageWithCreated>("Packages");
            // The following query gets packages created after 'lastCreated' and sorted by 'Created' ascending
            var queryOptionValue = String.Format("Created gt DateTime'{0}'", lastCreated.ToString(DateTimeFormatSpecifier));
            packagesQuery = packagesQuery.AddQueryOption("$orderby", "Created, Id, Version");
            packagesQuery = packagesQuery.AddQueryOption("$filter", queryOptionValue);
            Log.QueryFilter(queryOptionValue);
            return packagesQuery;
        }

        private string GetTempFolderPath(string sourceV2FeedName)
        {
            var tempFolderPath = Path.Combine(Path.GetTempPath(), sourceV2FeedName);
            if (Directory.Exists(tempFolderPath))
            {
                Directory.Delete(tempFolderPath, recursive: true);
            }
            Directory.CreateDirectory(tempFolderPath);
            return tempFolderPath;
        }

        /// <summary>
        /// Installs the package locally and pushes the package onto the destination server
        /// Uses OptimizedPackage much like the 'nuget push' command to prevent issues caused by
        /// holding onto package as a stream in memory
        /// </summary>
        private void MirrorPackage(DataServicePackageWithCreated package, PackageServer destinationServer, IPackageManager tempPackageManager, LocalPackageRepository tempLocalRepo,
        string apiKey, int timeOut)
        {
            // Download the package locally into a temp folder. This prevents storing the package in memory
            // which becomes an issue with large packages. Push command uses OptimizedZipPackage and we will too
            string localPackagePath = String.Empty;
            OptimizedZipPackage localPackage = null;

            try
            {
                Log.AddingPackageLocally(package.ToString(), package.Created.Value.DateTime.ToString());
                tempPackageManager.InstallPackage(package.Id, package.SemanticVersion, ignoreDependencies: true, allowPrereleaseVersions: true);
                Log.AddedPackageLocally(package.ToString());
            }
            catch (Exception ex)
            {
                throw new SourceException(ex);
            }

            try
            {
                // Push the local package onto destination Repository
                var localInstallPath = tempLocalRepo.PathResolver.GetInstallPath(package);
                localPackagePath = Path.Combine(localInstallPath, tempLocalRepo.PathResolver.GetPackageFileName(package));
                localPackage = new OptimizedZipPackage(localPackagePath);
                Log.PushingPackage(localPackage.ToString());
                destinationServer.PushPackage(apiKey, localPackage, new FileInfo(localPackagePath).Length, timeOut, disableBuffering: false);
            }
            catch (Exception ex)
            {
                throw new DestinationException(ex);
            }
        }

        private HttpStatusCode? GetHttpStatusCodeFrom(Exception ex)
        {
            if (ex == null || !(ex is WebException))
            {
                return null;
            }

            // throw if the response in the WebException is not a HttpWebResponse or if the statusCode of the response if not 'Conflict'
            var response = (ex as WebException).Response;
            if (!(response is HttpWebResponse))
            {
                return null;
            }

            return ((HttpWebResponse)response).StatusCode;
        }

        private void ThrowSourceExceptionIfNeeded(SourceException ex, ref int retries, DataServicePackageWithCreated package)
        {
            // TO BE DELETED
            if(ex.InnerException is PathTooLongException)
            {
                Log.LogMessage(String.Format("PathTooLongException on package {0}", package.ToString()));
                return;
            }

            HttpStatusCode? code = GetHttpStatusCodeFrom(ex.InnerException);

            switch(code)
            {
                // '403 Forbidden' from Source. For reasons unknown, certain listed packages are not available for download. Source returns "Access Denied"
                case HttpStatusCode.Forbidden:
                    if (retries < MaxRetries)
                        throw ex;

                    // Since, retries >= MaxRetries
                    // Set retries to and 0 and don't rethrow
                    // This accomplishes max retries and skipping to next package
                    retries = 0;
                    Log.SkippedForbiddenPackage(package.ToString());
                    break;

                // '404 Not Found' from Source. Package is available on the feed but not available for download already
                case HttpStatusCode.NotFound:
                // Any other code or if code is null. Throw
                default:
                    throw ex;
            }
        }

        private async Task ThrowDestinationExceptionIfNeeded(DestinationException ex, DataServicePackageWithCreated package, JObject mirrorJson, SqlConnectionStringBuilder cstr, CloudStorageAccount account)
        {
            var inner = ex.InnerException;
            HttpStatusCode? code = (inner != null && inner is InvalidOperationException) ? GetHttpStatusCodeFrom(inner.InnerException) : GetHttpStatusCodeFrom(inner);

            if (code == null || code != HttpStatusCode.Conflict)
            {
                throw ex;
            }

            switch (code)
            {
                // '409 Conflict' from Destination- Package already exists in destination. Don't rethrow
                case HttpStatusCode.Conflict:
                    var sourceJObject = GetJObject(mirrorJson, package.Id, package.SemanticVersion);
                    if (sourceJObject == null)
                    {
                        throw new InvalidOperationException("Package" + package.Id + "//" + package.SemanticVersion.ToString() + "is already mirrored, but, not present in mirror.json. WRONG!");
                    }
                    var oldSourceCreated = sourceJObject[SourceCreatedKey].Value<DateTime>();
                    var newSourceCreated = new DateTime(package.Created.Value.DateTime.Ticks, DateTimeKind.Utc);
                    if (!newSourceCreated.Equals(oldSourceCreated))
                    {
                        // This package while already mirrored to the destination, has been deleted from the source and created again to the source
                        // Hence, the different SourceCreated Date
                        // Need to delete the package
                        Log.DeletingOldRevision(package.ToString(), oldSourceCreated.ToString(DateTimeFormatSpecifier), newSourceCreated.ToString(DateTimeFormatSpecifier));
                        await NuGetV2RepositoryMirrorPackageDeletor.DeletePackage(cstr, account, sourceJObject, package.Id, package.SemanticVersion.ToString());
                        throw ex;
                    }
                    Log.PackageAlreadyExists(package.ToString());
                    break;

                // Any other code or if code is null. Throw
                default:
                    throw ex;
            }
        }

        private static int CompareToSourceCreated(JObject leftJObject, JObject rightJObject)
        {
            var ticks = rightJObject[SourceCreatedKey].Value<DateTime>().Ticks;
            return CompareToSourceCreated(leftJObject, ticks);
        }

        private static int CompareToSourceCreated(JObject jObject, long sourceCreatedTicks)
        {
            var sourceCreatedUtc = new DateTime(sourceCreatedTicks, DateTimeKind.Utc);
            var sourceJObjectSourceCreated = jObject[SourceCreatedKey].Value<DateTime>();
            return sourceJObjectSourceCreated.CompareTo(sourceCreatedUtc);
        }
        private static JObject GetJObject(JObject mirrorJson, string id, SemanticVersion version)
        {
            var packageIndex = mirrorJson[PackageIndexKey];
            return (JObject)packageIndex.Where(s => s[DeletedKey] == null && String.Equals(s[IdKey].ToString(), id, StringComparison.OrdinalIgnoreCase) && version.Equals(new SemanticVersion(s[VersionKey].ToString()))).SingleOrDefault();
        }

        private static bool IsMirrorJsonValid(JObject mirrorJson)
        {
            var packageIndex = mirrorJson[PackageIndexKey] as JArray;
            if (packageIndex == null)
            {
                return false;
            }

            if (packageIndex.Count > 1)
            {
                for (int i = 1; i < packageIndex.Count; i++)
                {
                    var leftObject = packageIndex[i - 1] as JObject;
                    var rightObject = packageIndex[i] as JObject;
                    if (leftObject == null || rightObject == null || CompareToSourceCreated(leftObject, rightObject) > 0)
                    {
                        // If the leftObject or rightObject is null or if SourceCreated of leftObject is greater than rightObject, mirrorJson is not valid
                        return false;
                    }
                }
            }
            return true;
        }

        private static JObject GetNewPackage(DataServicePackageWithCreated package)
        {
            JObject jObject = new JObject();
            var utcdt = new DateTime(package.Created.Value.DateTime.Ticks, DateTimeKind.Utc);
            jObject.Add(SourceCreatedKey, utcdt.ToString(DateTimeFormatSpecifier));
            jObject.Add(IdKey, package.Id);
            jObject.Add(VersionKey, package.SemanticVersion.ToString());
            jObject.Add(ListedKey, package.IsListed);
            // No need to add Deleted at this point
            return jObject;
        }

        private static JObject AddNewPackage(JObject mirrorJson, DataServicePackageWithCreated package)
        {
            // Should Check for prior existence of the package Id, version and delete the old one if SourceLastCreated is not the same
            // This logic is added in the 409 Conflict path
            var array = mirrorJson[PackageIndexKey] as JArray;
            if (array == null)
            {
                throw new InvalidOperationException("There is no array of 'packageIndex' in mirror json");
            }
            var lastItem = array.Count > 0 ? array[array.Count - 1] as JObject : null;
            if(lastItem != null)
            {
                if (CompareToSourceCreated(lastItem, package.Created.Value.DateTime.Ticks) > 0)
                {
                    throw new InvalidOperationException("Last package added has a greater SourceCreated Date than the new package being added");
                }
            }

            var jObject = GetNewPackage(package);
            array.Add(jObject);

            return jObject;
        }

        /// <summary>
        /// Queries for packages created after 'lastCreated'. At most, 40 packages may be returned
        /// Mirror that batch of packages to the destination server
        /// </summary>
        /// <returns>Returns lastMirroredPackage or null</returns>
        private DataServicePackageWithCreated QueryAndMirrorBatch(DataServices.DataServiceContext serviceContext, PackageServer destinationServer, DateTime lastCreated,
        string apiKey, int timeOut, JObject mirrorJson, ref int retries, ref int count, ref int skipIndex, SqlConnectionStringBuilder cstr, CloudStorageAccount account)
        {
            var newPackages = GetNewPackagesToMirror(serviceContext, lastCreated);

            // Push packages
            var tempFolderPath = GetTempFolderPath(serviceContext.BaseUri.DnsSafeHost);
            Log.TempFolderPath(tempFolderPath);

            var tempLocalRepo = new LocalPackageRepository(tempFolderPath);
            var tempPackageManager = new PackageManager(new DataServicePackageRepository(serviceContext.BaseUri), tempFolderPath);

            DataServicePackageWithCreated currentPackage = null;
            DataServicePackageWithCreated lastMirroredPackage = null;
            try
            {
                do
                {
                    // The following code deletes the temp folder if one exists and creates a new one
                    GetTempFolderPath(serviceContext.BaseUri.DnsSafeHost);
                    var newPackagesList = newPackages.Skip(skipIndex).ToList();
                    Log.PackagesCopyCount(newPackagesList.Count);

                    if (newPackagesList.Count == 0)
                    {
                        break;
                    }

                    foreach (DataServicePackageWithCreated package in newPackagesList)
                    {
                        try
                        {
                            currentPackage = package;
                            MirrorPackage(package, destinationServer, tempPackageManager, tempLocalRepo, apiKey, timeOut);
                            var jObject = AddNewPackage(mirrorJson, package);
                            if(!package.IsListed)
                            {
                                // The new package being pushed is not listed. Mark it as unlisted
                                NuGetV2RepositoryMirrorPackageDeletor.SetListed(cstr, jObject, package.Id, package.SemanticVersion.ToString(), false).Wait();
                            }
                            Log.PushedToDestination(++count);
                        }
                        catch (SourceException ex)
                        {
                            ThrowSourceExceptionIfNeeded(ex, ref retries, package);
                        }
                        catch (DestinationException ex)
                        {
                            ThrowDestinationExceptionIfNeeded(ex, package, mirrorJson, cstr, account).Wait();
                        }
                        lastMirroredPackage = package;
                        retries = 0;
                        ++skipIndex;
                    }
                } while (true);
            }
            catch (Exception ex)
            {
                retries++;
                Log.ServerUnreachable(retries, ex.Message);
                if (currentPackage != null)
                {
                    Log.MirrorFailed(currentPackage.ToString());
                }
            }

            // Delete the packages stored locally
            Directory.Delete(tempFolderPath, recursive: true);
            Log.DeletedTempFolder(tempFolderPath);

            return lastMirroredPackage;
        }

        private static async Task<JObject> GetJObject(CloudBlobContainer container, string blobName)
        {
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            string json = await blob.DownloadTextAsync();
            return JObject.Parse(json);
        }

        private static async Task SetJObject(CloudBlobContainer container, string blobName, JObject jObject)
        {
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            blob.Properties.ContentType = ContentTypeJson;
            await blob.UploadTextAsync(jObject.ToString());
        }
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-NuGetV2RepositoryMirrorer")]
    public class NuGetV2RepositoryMirrorerEventSource : EventSource
    {
        public static readonly NuGetV2RepositoryMirrorerEventSource Log = new NuGetV2RepositoryMirrorerEventSource();

        private NuGetV2RepositoryMirrorerEventSource() { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = @"lastCreated as retrieved from the lastCreated blob {0} is {1}, DateTimeKind: {2}.
So, packages will be mirrored from {3} to {4} whose created Date is greater than {1}")]
        public void PreparingToMirror(string blobName, string lastCreated, string dateTimeKind, string source, string destination) { WriteEvent(1, blobName, lastCreated, dateTimeKind, source, destination); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Message = "New lastCreated : {0}. End of Iteration")]
        public void EndOfIteration(string lastCreated) { WriteEvent(2, lastCreated); }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Message = "New LastCreated to be stored in {0} is {1}, DateTimeKind: {2}")]
        public void NewLastCreatedAtEndOfInvocation(string blobName, string lastCreated, string dateTimeKind) { WriteEvent(3, blobName, lastCreated, dateTimeKind); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Message = "Querying for packages created to {0} since {1}")]
        public void QueryNewPackages(string source, string lastCreated) { WriteEvent(4, source, lastCreated); }

        [Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Message = "Query Filter : {0}")]
        public void QueryFilter(string queryFilter) { WriteEvent(5, queryFilter); }

        [Event(
            eventId: 6,
            Level = EventLevel.Informational,
            Message = "Adding {0} locally. Created Date in Source : {1}")]
        public void AddingPackageLocally(string package, string createdDateInSource) { WriteEvent(6, package, createdDateInSource); }

        [Event(
            eventId: 7,
            Level = EventLevel.Informational,
            Message = "Added {0} locally")]
        public void AddedPackageLocally(string package) { WriteEvent(7, package); }

        [Event(
            eventId: 8,
            Level = EventLevel.Informational,
            Message = "Pushing the local package {0} to destination Repository")]
        public void PushingPackage(string package) { WriteEvent(8, package); }

        [Event(
            eventId: 9,
            Level = EventLevel.Informational,
            Message = "Number of packages to copy in this iteration : {0}")]
        public void PackagesCopyCount(int packagesCopiedCount) { WriteEvent(9, packagesCopiedCount); }

        [Event(
            eventId: 10,
            Level = EventLevel.Informational,
            Message = "Packages will be temporarily stored in : {0}")]
        public void TempFolderPath(string tempFolderPath) { WriteEvent(10, tempFolderPath); }

        [Event(
            eventId: 11,
            Level = EventLevel.Informational,
            Message = "Sorted new packages to be mirrored by Created Date in Source")]
        public void NewPackagesSortedByCreatedDate() { WriteEvent(11); }

        [Event(
            eventId: 12,
            Level = EventLevel.Informational,
            Message = "Pushed the package to destination repository. Total number of packages mirrored so far in this invocation : {0}")]
        public void PushedToDestination(int countSoFar) { WriteEvent(12, countSoFar); }

        [Event(
            eventId: 13,
            Level = EventLevel.Informational,
            Message = "Skipping mirroring of package {0}, since it already exists in destination")]
        public void PackageAlreadyExists(string package) { WriteEvent(13, package); }

        [Event(
            eventId: 14,
            Level = EventLevel.Informational,
            Message = "Need to retry mirroring, server is not reachable. Retries performed: {0}. Exception: {1}")]
        public void ServerUnreachable(int retries, string exception) { WriteEvent(14, retries, exception); }

        [Event(
            eventId: 15,
            Level = EventLevel.Informational,
            Message = "Could not mirror package {0}")]
        public void MirrorFailed(string package) { WriteEvent(15, package); }

        [Event(
            eventId: 16,
            Level = EventLevel.Informational,
            Message = "Deleted temp folder for packages: {0}")]
        public void DeletedTempFolder(string tempFolder) { WriteEvent(16, tempFolder); }

        [Event(
            eventId: 17,
            Level = EventLevel.Informational,
            Message = "Skipped package '{0}', since, its access is Forbidden even after max retries")]
        public void SkippedForbiddenPackage(string package) { WriteEvent(17, package); }

        [Event(
            eventId:18,
            Level=EventLevel.Informational,
            Message="GENERAL LOG: {0}")]
        public void LogMessage(string message) { WriteEvent(18, message); }

        [Event(
            eventId:19,
            Level=EventLevel.Informational,
            Message="Package {0} already exists in the mirror, but with an older source created date {1}. Current Source Created Date is {2}. This implies that the package was deleted and added back again. So, deleting the package in the mirror")]
        public void DeletingOldRevision(string package, string oldSourceCreated, string newSourceCreated) { WriteEvent(19, package, oldSourceCreated, newSourceCreated); }
    }
}
