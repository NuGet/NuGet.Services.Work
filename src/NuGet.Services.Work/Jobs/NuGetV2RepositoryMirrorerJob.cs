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
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using DataServices = NuGetCoreRef::System.Data.Services.Client;

namespace NuGet.Services.Work.Jobs
{
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
        private const string LastPublishedKey = "lastPublished";
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

        protected ConfigurationHub Config { get; private set; }

        private CloudBlobContainer MirrorBlobContainer { get; set; }
        private string UserAgent { get; set; }

        public NuGetV2RepositoryMirrorerJob(ConfigurationHub config)
        {
            Config = config;
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

            var jObject = await GetJObject(MirrorBlobContainer, MirrorBlobName);

            var oldLastPublished = jObject.Value<DateTime>(LastPublishedKey);
            var lastPublished = oldLastPublished;
            Log.PreparingToMirror(MirrorBlobName, lastPublished.ToString(DateTimeFormatSpecifier), lastPublished.Kind.ToString(), sourceUri.AbsoluteUri, DestinationUri);

            int retries = 0;
            int count = 0;
            Exception caughtException = null;

            //
            //  POSSIBLE ACTIONS when an error is encountered
            //  A) Always Skip to next package
            //  B) Retry 'MaxRetries' times and skip to next package
            //  C) Retry 'MaxRetries' times and fail
            //
            //	KNOWN ERRORS
            //	1) '409 Conflict' from Destination- Package already exists in destination
            //		i)  Action: (A). Always Skip to next package
            //      ii) Before skipping, Update lastMirroredPackage.Published locally, i.e inside the while loop
            //	2) '403 Forbidden' from Source. For reasons unknown, certain listed packages are not available for download. Source returns "Access Denied"
            //      i)  Action: (B). Retry 'MaxRetries' times and Skip to next package
            //      ii) Log every retry. Before skipping, Update lastMirroredPackage.Published locally, i.e inside the while loop
            //  3) '404 Not Found' from Source. Package is available on the feed but not available for download already
            //      i)  Action: (C). Retry 'MaxRetries' times and Fail
            //      ii) Log every retry. Update lastMirroredPackage.Published in blob storage
            //	4) Unknown Error
            //      i)  Action: (C). Retry 'MaxRetries' times and Fail
            //      ii) Log every retry. Update lastMirroredPackage.Published in blob storage
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

                    // Query for packages published since lastPublished
                    PackageServer destinationServer = new PackageServer(DestinationUri, UserAgent);

                    var lastMirroredPackage = QueryAndMirrorBatch(serviceContext, destinationServer, lastPublished, ApiKey, timeOutPerPush.Milliseconds, ref retries, ref count);
                    if (lastMirroredPackage != null)
                    {
                        if (!lastMirroredPackage.Published.HasValue)
                        {
                            throw new InvalidOperationException("Last mirrored package : " + lastMirroredPackage.ToString() + "has a null Published Time.. WRONG!!!");
                        }

                        // Note that the Published DateTime is always stored in UTC, but the DateTimeKind of the value obtained is Unspecified
                        // So, store it as UTC
                        lastPublished = new DateTime(lastMirroredPackage.Published.Value.DateTime.Ticks, DateTimeKind.Utc);
                        Log.EndOfIteration(lastPublished.ToString(DateTimeFormatSpecifier));
                    }
                    else if (retries == 0 || retries > MaxRetries)
                    {
                        break;
                    }
                } while (true);
            }
            catch (Exception ex)
            {
                // Catch the exception here so that new lastPublished is always stored
                // We can throw this exception at the end
                caughtException = ex;
            }

            if (!oldLastPublished.Equals(lastPublished))
            {
                jObject[LastPublishedKey] = lastPublished.ToString(DateTimeFormatSpecifier);
                await SetJObject(MirrorBlobContainer, MirrorBlobName, jObject);
                Log.NewLastPublishedAtEndOfInvocation(MirrorBlobName, lastPublished.ToString(DateTimeFormatSpecifier), lastPublished.Kind.ToString());
            }

            if (caughtException != null)
            {
                throw caughtException;
            }
        }

        private List<DataServicePackage> GetNewPackagesToMirror(DataServices.DataServiceContext serviceContext, DateTime lastPublished)
        {
            Log.QueryNewPackages(serviceContext.BaseUri.AbsoluteUri, lastPublished.ToString(DateTimeFormatSpecifier));
            var packagesQuery = serviceContext.CreateQuery<DataServicePackage>("Packages");
            // The following query gets packages published after 'lastPublished' and sorted by 'Published' ascending
            var queryOptionValue = String.Format("Published gt DateTime'{0}'", lastPublished.ToString(DateTimeFormatSpecifier));
            packagesQuery = packagesQuery.AddQueryOption("$orderby", "Published");
            packagesQuery = packagesQuery.AddQueryOption("$filter", queryOptionValue);
            Log.QueryFilter(queryOptionValue);
            return packagesQuery.ToList();
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
        private void MirrorPackage(IPackage package, PackageServer destinationServer, IPackageManager tempPackageManager, LocalPackageRepository tempLocalRepo,
        string apiKey, int timeOut)
        {
            // Download the package locally into a temp folder. This prevents storing the package in memory
            // which becomes an issue with large packages. Push command uses OptimizedZipPackage and we will too
            string localPackagePath = String.Empty;
            OptimizedZipPackage localPackage = null;

            try
            {
                Log.AddingPackageLocally(package.ToString(), package.Published.Value.DateTime.ToString());
                tempPackageManager.InstallPackage(package.Id, package.Version, ignoreDependencies: true, allowPrereleaseVersions: true);
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
                destinationServer.PushPackage(apiKey, localPackage, new FileInfo(localPackagePath).Length, timeOut);
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

        private void ThrowSourceExceptionIfNeeded(SourceException ex, ref int retries, IPackage package)
        {
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

        private void ThrowDestinationExceptionIfNeeded(DestinationException ex, ref int retries, IPackage package)
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
                    Log.PackageAlreadyExists(package.ToString());
                    break;

                // Any other code or if code is null. Throw
                default:
                    throw ex;
            }
        }

        /// <summary>
        /// Queries for packages published after 'lastPublished'. At most, 40 packages may be returned
        /// Mirror that batch of packages to the destination server
        /// </summary>
        /// <returns>Returns lastMirroredPackage or null</returns>
        private IPackage QueryAndMirrorBatch(DataServices.DataServiceContext serviceContext, PackageServer destinationServer, DateTime lastPublished,
        string apiKey, int timeOut, ref int retries, ref int count)
        {
            var newPackages = GetNewPackagesToMirror(serviceContext, lastPublished);
            Log.PackagesCopiedCount(newPackages.Count);

            if (newPackages.Count == 0)
            {
                return null;
            }

            // Push packages
            var tempFolderPath = GetTempFolderPath(serviceContext.BaseUri.DnsSafeHost);
            Log.TempFolderPath(tempFolderPath);

            var tempLocalRepo = new LocalPackageRepository(tempFolderPath);
            var tempPackageManager = new PackageManager(new DataServicePackageRepository(serviceContext.BaseUri), tempFolderPath);

            // Packages returned are not sorted by Published Date but by name
            // Sort them by Published Date in ascending order so that the packages are pushed to the mirror in that order
            newPackages.Sort(delegate(DataServicePackage x, DataServicePackage y) { return Nullable.Compare<DateTimeOffset>(x.Published, y.Published); });
            Log.NewPackagesSortedByPublishedDate();

            IPackage currentPackage = null;
            IPackage lastMirroredPackage = null;
            try
            {
                foreach (IPackage package in newPackages)
                {
                    try
                    {
                        currentPackage = package;
                        MirrorPackage(package, destinationServer, tempPackageManager, tempLocalRepo, apiKey, timeOut);
                        Log.PushedToDestination(++count);
                    }
                    catch(SourceException ex)
                    {
                        ThrowSourceExceptionIfNeeded(ex, ref retries, package);
                    }
                    catch (DestinationException ex)
                    {
                        ThrowDestinationExceptionIfNeeded(ex, ref retries, package);
                    }

                    lastMirroredPackage = package;
                    retries = 0;
                }
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

        private async Task<JObject> GetJObject(CloudBlobContainer container, string blobName)
        {
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            string json = await blob.DownloadTextAsync();
            return JObject.Parse(json);
        }

        private async Task SetJObject(CloudBlobContainer container, string blobName, JObject jObject)
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
            Message = @"lastPublished as retrieved from the lastPublished blob {0} is {1}, DateTimeKind: {2}.
So, packages will be mirrored from {3} to {4} whose published Date is greater than {1}")]
        public void PreparingToMirror(string blobName, string lastPublished, string dateTimeKind, string source, string destination) { WriteEvent(1, blobName, lastPublished, dateTimeKind, source, destination); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Message = "New lastPublished : {0}. End of Iteration")]
        public void EndOfIteration(string lastPublished) { WriteEvent(2, lastPublished); }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Message = "New LastPublished to be stored in {0} is {1}, DateTimeKind: {2}")]
        public void NewLastPublishedAtEndOfInvocation(string blobName, string lastPublished, string dateTimeKind) { WriteEvent(3, blobName, lastPublished, dateTimeKind); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Message = "Querying for packages published to {0} since {1}")]
        public void QueryNewPackages(string source, string lastPublished) { WriteEvent(4, source, lastPublished); }

        [Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Message = "Query Filter : {0}")]
        public void QueryFilter(string queryFilter) { WriteEvent(5, queryFilter); }

        [Event(
            eventId: 6,
            Level = EventLevel.Informational,
            Message = "Adding {0} locally. Published Date in Source : {1}")]
        public void AddingPackageLocally(string package, string publishedDateInSource) { WriteEvent(6, package, publishedDateInSource); }

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
        public void PackagesCopiedCount(int packagesCopiedCount) { WriteEvent(9, packagesCopiedCount); }

        [Event(
            eventId: 10,
            Level = EventLevel.Informational,
            Message = "Packages will be temporarily stored in : {0}")]
        public void TempFolderPath(string tempFolderPath) { WriteEvent(10, tempFolderPath); }

        [Event(
            eventId: 11,
            Level = EventLevel.Informational,
            Message = "Sorted new packages to be mirrored by Published Date in Source")]
        public void NewPackagesSortedByPublishedDate() { WriteEvent(11); }

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
    }
}
