extern alias NuGetCoreRef;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
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
    public class NuGetV2RepositoryMirrorDeletor
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

        private static int GetMidIndex(List<MinPackage> destinationPackages, int startIndex, int endIndex, DateTime mid)
        {
            int midIndex = -1;
            for(int i = startIndex; i < endIndex; i++)
            {
                if (destinationPackages[i].SourcePublished >= mid)
                {
                    midIndex = i;
                    break;
                }
            }

            if (midIndex == -1)
            {
                throw new InvalidOperationException("WRONG. MidIndex cannot be -1");
            }
            return midIndex;
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

        public static List<int> GetDeletedPackageIndices(Uri sourceUri, List<MinPackage> destinationPackages, int startIndex, int endIndex, DateTime start, DateTime end)
        {
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

            // At this point, diffCount is greater than 0 and leftCount is not 0
            // There should be some packages to be returned
            var halfSpan = new TimeSpan((end - start).Ticks / 2);
            var mid = start.Add(halfSpan);

            var midIndex = GetMidIndex(destinationPackages, startIndex, endIndex, mid);

            var left = GetDeletedPackageIndices(sourceUri, destinationPackages, startIndex, midIndex, start, mid);
            var right = GetDeletedPackageIndices(sourceUri, destinationPackages, midIndex, endIndex, mid, end);

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

            // Set 'start' as the SourcePublished of the first destination package
            var start = destinationPackages[0].SourcePublished;
            // Set 'end' as the SourcePublished of the last destination package
            var end = destinationPackages[destinationPackages.Count - 1].SourcePublished.AddSeconds(1);

            var deletedPackageIndices = GetDeletedPackageIndices(sourceUri, destinationPackages, 0, destinationPackages.Count, start, end);

            var list = new List<MinPackage>();
            foreach (var index in deletedPackageIndices)
            {
                list.Add(destinationPackages[index]);
            }

            return list;
        }
    }
}
