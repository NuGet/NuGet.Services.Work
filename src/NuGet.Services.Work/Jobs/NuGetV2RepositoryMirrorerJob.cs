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
    [Description("Job to mirror a NuGet V2 Repository by polling on a defined interval")]
    public class NuGetV2RepositoryMirrorerJob : JobHandler<NuGetV2RepositoryMirrorerEventSource>
    {
        private const string LastPublishedKey = "lastPublished";
        private const string ContentTypeJson = "application/json";
        private const string DateTimeFormatSpecifier = "O";
        private const int PushTimeOutInMinutes = 5;
        private const string DefaultMirrorBlobContainerName = "mirror";
        private const string DefaultMirrorBlobName = "Mirror.json";

        public string SourceV2FeedName { get; set; }
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
            if (String.IsNullOrEmpty(SourceV2FeedName))
            {
                throw new ArgumentException("SourceV2FeedName cannot be null or empty");
            }

            if (String.IsNullOrEmpty(DestinationUri))
            {
                throw new ArgumentException("DestinationUri cannot be null or empty");
            }

            if (String.IsNullOrEmpty(ApiKey))
            {
                throw new ArgumentException("ApiKey cannot be null or empty");
            }

            // Arrange or set defaults for parameters that are not provided
            UserAgent = String.Format("{0}v2FeedMirrorer", SourceV2FeedName);
            var timeOutPerPush = TimeSpan.FromMinutes(PushTimeOutInMinutes);
            MirrorStorage = MirrorStorage ?? Config.Storage.Legacy;

            if (MirrorStorage == null)
            {
                throw new ArgumentNullException("MirrorStorage", "Mirror storage is not provided or present in the config");
            }

            MirrorBlobContainerName = String.IsNullOrEmpty(MirrorBlobContainerName) ? DefaultMirrorBlobContainerName : MirrorBlobContainerName;
            MirrorBlobContainer = MirrorStorage.CreateCloudBlobClient().GetContainerReference(MirrorBlobContainerName);
            MirrorBlobName = String.IsNullOrEmpty(MirrorBlobName) ? DefaultMirrorBlobName : MirrorBlobName;

            var sourceUri = new Uri("http://" + SourceV2FeedName + "/api/v2/");
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

            //	POSSIBLE ERRORS
            //	1) Conflict- Package already exists in destination
            //		Action: Skip to next package to mirror. Update lastMirroredPackage.Published locally, i.e inside the while loop
            //	2) Package is available on the feed but not available for download already. This happens quite a bit for packages just published to the Source
            //		Action: 5 retries allowed. Log every retry. It is a retry only if a package could not be downloaded and mirrored. Update lastMirroredPackage.Published in  blob storage
            //	3) Unknown Error
            //		Action: Fault job. First, store the new lastPublished.Published, if any, in the blob and throw the unknown exception to fault the job
            try
            {
                do
                {
                    if ((Invocation.NextVisibleAt - DateTimeOffset.UtcNow) < TimeSpan.FromMinutes(120))
                    {
                        // There are at most 40 packages returned by a single query (which will get downloaded and pushed)
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
                    else if (retries == 0 || retries > 5)
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
            var queryOptionValue = String.Format("Published gt DateTime'{0}'", lastPublished.ToString(DateTimeFormatSpecifier));
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
            Log.AddingPackageLocally(package.ToString(), package.Published.Value.DateTime.ToString());
            tempPackageManager.InstallPackage(package.Id, package.Version, ignoreDependencies: true, allowPrereleaseVersions: true);
            Log.AddedPackageLocally(package.ToString());

            // Push the local package onto destination Repository
            var localInstallPath = tempLocalRepo.PathResolver.GetInstallPath(package);
            var localPackagePath = Path.Combine(localInstallPath, tempLocalRepo.PathResolver.GetPackageFileName(package));
            var localPackage = new OptimizedZipPackage(localPackagePath);

            Log.PushingPackage(localPackage.ToString());
            destinationServer.PushPackage(apiKey, localPackage, new FileInfo(localPackagePath).Length, timeOut);
        }

        private void ThrowIfStatusCodeIsNotConflict(Exception ex)
        {
            // throw if the InnerException is null or if it not a WebException
            if (ex.InnerException == null || !(ex.InnerException is WebException))
            {
                throw ex;
            }

            // throw if the response in the WebException is not a HttpWebResponse or if the statusCode of the response if not 'Conflict'
            var response = (ex.InnerException as WebException).Response;
            if (!(response is HttpWebResponse) || ((HttpWebResponse)response).StatusCode != HttpStatusCode.Conflict)
            {
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

            IPackage lastMirroredPackage = null;
            try
            {
                foreach (IPackage package in newPackages)
                {
                    try
                    {
                        MirrorPackage(package, destinationServer, tempPackageManager, tempLocalRepo, apiKey, timeOut);
                        Log.PushedToDestination(++count);
                        lastMirroredPackage = package;
                        retries = 0;
                    }
                    catch (InvalidOperationException ex)
                    {
                        // Throw if InnerException is not a WebException OR if the StatusCode of the HttpWebResponse in the Web exception is not 'Conflict'
                        ThrowIfStatusCodeIsNotConflict(ex);

                        // The package has already been mirrored. Hence, the status Code 'Conflict'
                        Log.PackageAlreadyExists(package.ToString());

                        // Skip to next package. But, set lastMirroredPackage before doing so
                        lastMirroredPackage = package;
                    }
                }
            }
            catch (Exception ex)
            {
                retries++;
                Log.ServerUnreachable(retries, ex.Message);
                if (lastMirroredPackage != null)
                {
                    Log.MirrorFailed(lastMirroredPackage.ToString());
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
            Message = "Need to retry mirroring since all the new packages published may not be available for mirroring or the remote server is not reachable. Retries performed: {0}. Exception: {1}")]
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
    }
}
