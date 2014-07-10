extern alias NuGetCoreRef;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json.Linq;
using NuGet.Services.Work.Helpers;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using DataServices = NuGetCoreRef::System.Data.Services.Client;

namespace NuGet.Services.Work.Jobs
{
    public class NuGetV2RepositoryMirrorPackageDeletor
    {
        private class MinPackage : IComparable<MinPackage>
        {
            public const string IdKey = "id";
            public const string VersionKey = "version";
            public const string SourceCreatedKey = "sourceCreated";
            public string Id { get; set; }
            public SemanticVersion SemanticVersion { get; set; }
            public DateTime SourceCreated { get; set; }
            public JObject SourceJObject { get; set; }

            public int CompareTo(MinPackage other)
            {
                return this.SourceCreated.CompareTo(other.SourceCreated);
            }

            public static MinPackage GetMinPackage(JObject jObject)
            {
                var minPackage = new MinPackage();
                minPackage.Id = jObject[IdKey].ToString();
                minPackage.SemanticVersion = new SemanticVersion(jObject[VersionKey].ToString());
                var sourceCreated = jObject[SourceCreatedKey].Value<DateTime>();
                minPackage.SourceCreated = new DateTime(sourceCreated.Ticks, DateTimeKind.Utc);

                minPackage.SourceJObject = jObject;
                return minPackage;
            }

            public override string ToString()
            {
                const string stringFormat = "{0}/{1}/{2}";
                return String.Format(stringFormat, Id, SemanticVersion.ToString(), SourceCreated.ToString("O"));
            }
        }
        public const string PackageIndexKey = "packageIndex";
        public const string DeletedKey = "deleted";
        public const string ListedKey = "listed";
        private const string CountDateTimeRangeFormat = "Packages/$count/?$filter=Created ge DateTime'{0}' and Created lt DateTime'{1}'&$orderby=Created";

        private static int GetPackagesCount(Uri v2Feed, DateTime start, DateTime end)
        {
            Uri fullUri = new Uri(v2Feed, String.Format(CountDateTimeRangeFormat, start.ToString("o"), end.ToString("o")));
            WebRequest request = WebRequest.Create(fullUri);
            var response = request.GetResponse();
            string responseString = String.Empty;
            using (var streamReader = new StreamReader(response.GetResponseStream()))
            {
                responseString = streamReader.ReadToEnd();
            }
            return Int32.Parse(responseString);
        }

        private static List<int> GetDeletedPackageIndices(Uri sourceUri, List<MinPackage> destinationPackages, int startIndex, int endIndex, DateTime dateTime)
        {
            var serviceContext = new DataServices.DataServiceContext(sourceUri)
            {
                MergeOption = DataServices.MergeOption.OverwriteChanges,
                IgnoreMissingProperties = true,
            };
            var packagesQuery = serviceContext.CreateQuery<DataServicePackageWithCreated>("Packages");
            var queryOptionValue = String.Format("Created eq DateTime'{0}'", dateTime.ToString("o"));
            packagesQuery = packagesQuery.AddQueryOption("$orderby", "Id");
            packagesQuery = packagesQuery.AddQueryOption("$orderby", "Version");
            packagesQuery = packagesQuery.AddQueryOption("$filter", queryOptionValue);

            var skipIndex = 0;
            var sourcePackages = new List<DataServicePackageWithCreated>();
            do
            {
                var list = packagesQuery.Skip(skipIndex).ToList();
                if(list.Count == 0)
                    break;
                sourcePackages.AddRange(list);
                skipIndex = skipIndex + list.Count;
            }while(true);

            if(sourcePackages.Count == 0)
            {
                throw new InvalidOperationException("Don't call this method when it is already known that there are no source packages");
            }

            var deletedPackageIndices = new List<int>();
            for(int i = startIndex; i < endIndex; i++)
            {
                var destPackage = destinationPackages[i];
                var destPackageInSource = sourcePackages.Where(s => String.Equals(s.Id, destPackage.Id) && s.SemanticVersion.Equals(destPackage.SemanticVersion)).SingleOrDefault();
                if(destPackageInSource == null)
                {
                    // This destination package is not present in source
                    // Add index to list of deleted packages
                    deletedPackageIndices.Add(i);
                }
            }

            return deletedPackageIndices;
        }

        private static List<int> GetDeletedPackageIndices(Uri sourceUri, List<MinPackage> destinationPackages, int startIndex, int endIndex)
        {
            // Set 'start' as the SourceCreated of destination package at 'startIndex'
            var start = destinationPackages[startIndex].SourceCreated;

            // Search is (>= start && < end), in OData words (Packages/$count/?$filter=Created ge DateTime'{0}' and Created lt DateTime'{1}')
            // So, set end to SourceCreated of destination package at 'endIndex'. This ensures that the result from the source has a package
            // corresponding to package at 'endIndex - 1'. package at 'startIndex' is accounted for since search is >= start
            var end = endIndex != destinationPackages.Count ? destinationPackages[endIndex].SourceCreated : destinationPackages[endIndex - 1].SourceCreated.AddSeconds(1);

            var EmptyPackages = new List<int>();
            if (start > end)
            {
                return EmptyPackages;
            }

            if(start == end)
            {
                NuGetV2RepositoryMirrorPackageDeletorEventSource.Log.StartEqualsEnd(start.ToString("O"), (endIndex - startIndex));
                return GetDeletedPackageIndices(sourceUri, destinationPackages, startIndex, endIndex, start);
            }

            var leftCount = GetPackagesCount(sourceUri, start, end);
            var rightCount = endIndex - startIndex;

            if (leftCount == rightCount)
            {
                return EmptyPackages;
            }

            if (rightCount < leftCount)
            {
                throw new InvalidOperationException("leftCount cannot be greater than rightCount. That should mean some packages are not mirrored yet");
            }

            if (leftCount == 0)
            {
                // return rightOnlyPackages
                var list = new List<int>();
                for(int i = startIndex; i < endIndex; i++)
                {
                    list.Add(i);
                }

                return list;
            }

            var diffCount = rightCount - leftCount;
            NuGetV2RepositoryMirrorPackageDeletorEventSource.Log.DiffCountBetween(start.ToString("O"), end.ToString("O"), diffCount);

            var midIndex = (startIndex + endIndex) / 2;
            var mid = destinationPackages[midIndex].SourceCreated;

            var left = GetDeletedPackageIndices(sourceUri, destinationPackages, startIndex, midIndex);
            var right = GetDeletedPackageIndices(sourceUri, destinationPackages, midIndex, endIndex);

            if (diffCount != (left.Count + right.Count))
            {
                throw new InvalidOperationException("Missed something");
            }

            left.AddRange(right);
            return left;
        }

        private static List<MinPackage> GetSortedMinPackages(JObject mirrorJson, bool ignoreDeleted)
        {
            var list = new List<MinPackage>();
            var arrayObject = mirrorJson[PackageIndexKey];
            if (arrayObject == null)
            {
                return list;
            }

            var array = arrayObject as JArray;
            foreach (var item in array)
            {
                var jObject = item as JObject;
                if (jObject != null)
                {
                    var minPackage = MinPackage.GetMinPackage(jObject);
                    var deletedToken = jObject[DeletedKey];
                    if (!ignoreDeleted || deletedToken == null)
                    {
                        list.Add(minPackage);
                    }
                }
            }

            list.Sort();
            return list;
        }

        private static List<MinPackage> GetDeletedPackages(Uri sourceUri, JObject mirrorJson)
        {
            var destinationPackages = GetSortedMinPackages(mirrorJson, ignoreDeleted: true);

            if (destinationPackages.Count == 0)
            {
                return destinationPackages;
            }

            var deletedPackageIndices = GetDeletedPackageIndices(sourceUri, destinationPackages, 0, destinationPackages.Count);

            var list = new List<MinPackage>();
            foreach (var index in deletedPackageIndices)
            {
                list.Add(destinationPackages[index]);
            }

            return list;
        }

        public static async Task DeletePackage(SqlConnectionStringBuilder cstr, CloudStorageAccount account, JObject sourceJObject, string id, string version)
        {
            using (var connection = new SqlConnection(cstr.ConnectionString))
            {
                await connection.OpenAsync();
                await DeletePackage(connection, account, sourceJObject, id, version);
            }
        }

        public static async Task DeletePackage(SqlConnection connection, CloudStorageAccount account, JObject sourceJObject, string id, string version)
        {
            var dynamicPackages = await PackageDeletor.GetDeletePackages(connection, id, version, allVersions: false);
            foreach (var dynamicPackage in dynamicPackages)
            {
                NuGetV2RepositoryMirrorPackageDeletorEventSource.Log.DeletingPackage(sourceJObject.ToString());
                await PackageDeletor.DeletePackage(dynamicPackage, connection, account);
                var deletedTime = DateTime.UtcNow.ToString("O");
                sourceJObject[DeletedKey] = deletedTime;
                NuGetV2RepositoryMirrorPackageDeletorEventSource.Log.DeletedPackage(sourceJObject.ToString(), deletedTime);
            }
        }

        public static async Task DeletePackages(Uri sourceUri, JObject mirrorJson, CloudStorageAccount account, SqlConnectionStringBuilder cstr)
        {
            var minPackagesToBeDeleted = GetDeletedPackages(sourceUri, mirrorJson);
            NuGetV2RepositoryMirrorPackageDeletorEventSource.Log.TotalNumberOfPackagesToBeDeleted(minPackagesToBeDeleted.Count);
            using(var connection = new SqlConnection(cstr.ConnectionString))
            {
                await connection.OpenAsync();
                foreach(var minPackage in minPackagesToBeDeleted)
                {
                    await DeletePackage(connection, account, minPackage.SourceJObject, minPackage.Id, minPackage.SemanticVersion.ToString());
                }
            }
        }

        public static async Task SetListed(SqlConnectionStringBuilder cstr, string id, string version, bool isListed)
        {
            using(var connection = new SqlConnection(cstr.ConnectionString))
            {
                await connection.OpenAsync();
                await PackageDeletor.SetListed(connection, id, version, isListed);
            }
        }
    }

    [EventSource(Name = "Outercurve-NuGet-Services-NuGetV2RepositoryMirrorPackageDeletor")]
    public class NuGetV2RepositoryMirrorPackageDeletorEventSource : EventSource
    {
        public static readonly NuGetV2RepositoryMirrorPackageDeletorEventSource Log = new NuGetV2RepositoryMirrorPackageDeletorEventSource();

        private NuGetV2RepositoryMirrorPackageDeletorEventSource() { }

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Difference in count between startDate {0} and endDate {1} is {2}")]
        public void DiffCountBetween(string startDate, string endDate, int diffCount) { WriteEvent(1, startDate, endDate, diffCount); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Message = "'start' == 'end'. It is {0}. Simply iterate through the packages. DiffCount = {1}")]
        public void StartEqualsEnd(string startAndEnd, int diffCount) { WriteEvent(2, startAndEnd, diffCount); }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Message = "Total number of packages to be deleted : {0}")]
        public void TotalNumberOfPackagesToBeDeleted(int totalDiffCount) { WriteEvent(3, totalDiffCount); }

        [Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Message = "Deleting package {0}...")]
        public void DeletingPackage(string package) { WriteEvent(4, package); }

        [Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Message = "Deleted package {0}. Delete time is {1}")]
        public void DeletedPackage(string package, string deleteTime) { WriteEvent(5, package, deleteTime); }
    }
}
