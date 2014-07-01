extern alias NuGetCoreRef;
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
        public string PackageId { get; set; }
        public SemanticVersion Version { get; set; }
        public DateTime SourcePublished { get; set; }
        public DateTime? Deleted { get; set; }
    
        public int CompareTo(MinPackage other)
        {
 	        return this.SourcePublished.CompareTo(other.SourcePublished);
        }
    }
    public class NuGetV2RepositoryMirrorDeletor
    {
        private const string CountQueryOptionFormat = "Packages/$count/?$filter=Published ge DateTime'{0}' and Published lt DateTime'{1}'";
        private static int GetPackagesCount(Uri v2Feed, DateTime start, DateTime end)
        {
            Uri fullUri = new Uri(v2Feed, String.Format(CountQueryOptionFormat, start.ToString("o"), end.ToString("o")));
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
            return midIndex;
        }

        public static List<int> GetDeletedPackages(Uri sourceUri, List<MinPackage> destinationPackages, int startIndex, int endIndex, DateTime start, DateTime end)
        {
            var EmptyPackages = new List<int>();

            if (start >= end)
            {
                return EmptyPackages;
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

            var left = GetDeletedPackages(sourceUri, destinationPackages, startIndex, midIndex, start, mid);
            var right = GetDeletedPackages(sourceUri, destinationPackages, midIndex, endIndex, mid, end);

            if (diffCount != (left.Count + right.Count))
            {
                throw new InvalidOperationException("Missed something");
            }

            left.AddRange(right);
            return left;
        }
    }
}
