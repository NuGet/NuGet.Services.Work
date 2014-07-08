extern alias NuGetCoreRef;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using DataServices = NuGetCoreRef::System.Data.Services.Client;

namespace NuGet.Services.Work.Jobs
{
    public class MinPackage : IComparable<MinPackage>
    {
        public const string IdKey = "id";
        public const string VersionKey = "version";
        public const string SourcePublishedKey = "sourcePublished";
        public const string DeletedKey = "deleted";
        public string Id { get; set; }
        public SemanticVersion Version { get; set; }
        public DateTime SourcePublished { get; set; }
        public DateTime? Deleted { get; set; }
    
        public int CompareTo(MinPackage other)
        {
 	        return this.SourcePublished.CompareTo(other.SourcePublished);
        }

        public static MinPackage GetMinPackage(JObject jObject)
        {
            var minPackage = new MinPackage();
            minPackage.Id = jObject[IdKey].ToString();
            minPackage.Version = new SemanticVersion(jObject[VersionKey].ToString());
            var sourcePublished = jObject[SourcePublishedKey].Value<DateTime>();
            minPackage.SourcePublished = new DateTime(sourcePublished.Ticks, DateTimeKind.Utc);

            var deletedToken = jObject[DeletedKey];
            if(deletedToken != null)
            {
                var deleted = jObject[DeletedKey].Value<DateTime>();
                minPackage.Deleted = new DateTime(deleted.Ticks, DateTimeKind.Utc);
            }

            return minPackage;
        }

        public override string ToString()
        {
            const string stringFormat = "{0}/{1}/{2}/{3}";
            const string nullString = "NULL";
            return String.Format(stringFormat, Id, Version.ToString(), SourcePublished.ToString("O"), Deleted.HasValue ? Deleted.Value.ToString("O") : nullString);
        }
    }
    public class NuGetV2RepositoryMirrorPackageDeletor
    {
        public const string PackageIndexKey = "packageIndex";
        private const string CountDateTimeRangeFormat = "Packages/$count/?$filter=Published ge DateTime'{0}' and Published lt DateTime'{1}'";

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

        public static List<int> GetDeletedPackageIndices(Uri sourceUri, List<MinPackage> destinationPackages, int startIndex, int endIndex, DateTime dateTime)
        {
            var serviceContext = new DataServices.DataServiceContext(sourceUri)
            {
                MergeOption = DataServices.MergeOption.OverwriteChanges,
                IgnoreMissingProperties = true,
            };
            var packagesQuery = serviceContext.CreateQuery<DataServicePackage>("Packages");
            var queryOptionValue = String.Format("Published eq DateTime'{0}'", dateTime.ToString("o"));
            packagesQuery = packagesQuery.AddQueryOption("$orderby", "Id");
            packagesQuery = packagesQuery.AddQueryOption("$orderby", "Version");
            packagesQuery = packagesQuery.AddQueryOption("$filter", queryOptionValue);

            var skipIndex = 0;
            var sourcePackages = new List<DataServicePackage>();
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
                var destPackageInSource = sourcePackages.Where(s => String.Equals(s.Id, destPackage.Id) && s.Version.Equals(destPackage.Version)).Single();
                if(destPackageInSource == null)
                {
                    // This destination package is not present in source
                    // Add index to list of deleted packages
                    deletedPackageIndices.Add(i);
                }
            }

            return deletedPackageIndices;
        }

        public static List<int> GetDeletedPackageIndices(Uri sourceUri, List<MinPackage> destinationPackages, int startIndex, int endIndex)
        {
            // Set 'start' as the SourcePublished of destination package at 'startIndex'
            var start = destinationPackages[startIndex].SourcePublished;

            // Search is (>= start && < end), in OData words (Packages/$count/?$filter=Published ge DateTime'{0}' and Published lt DateTime'{1}')
            // So, set end to SourcePublished of destination package at 'endIndex'. This ensures that the result from the source has a package
            // corresponding to package at 'endIndex - 1'. package at 'startIndex' is accounted for since search is >= start
            var end = endIndex != destinationPackages.Count ? destinationPackages[endIndex].SourcePublished : destinationPackages[endIndex - 1].SourcePublished.AddSeconds(1);

            var EmptyPackages = new List<int>();
            if (start > end)
            {
                return EmptyPackages;
            }

            if(start == end)
            {
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

            var midIndex = (startIndex + endIndex) / 2;
            var mid = destinationPackages[midIndex].SourcePublished;

            var left = GetDeletedPackageIndices(sourceUri, destinationPackages, startIndex, midIndex);
            var right = GetDeletedPackageIndices(sourceUri, destinationPackages, midIndex, endIndex);

            if (diffCount != (left.Count + right.Count))
            {
                throw new InvalidOperationException("Missed something");
            }

            left.AddRange(right);
            return left;
        }

        public static List<MinPackage> GetSortedMinPackages(JObject mirrorJson, bool ignoreDeleted)
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
                    if (!ignoreDeleted || !minPackage.Deleted.HasValue)
                    {
                        list.Add(minPackage);
                    }
                }
            }

            list.Sort();
            return list;
        }

        public static List<MinPackage> GetDeletedPackages(Uri sourceUri, JObject mirrorJson)
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
    }

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
            Message = "'start' == 'end'. It is {0}. Simply iterate through the packages. DiffCount = {1}")]
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
