using Newtonsoft.Json.Linq;
using NuGet.Services.Work.Jobs;
using NuGet.Services.Work.Jobs.Models;
using System;
using System.Collections.Generic;
using Xunit;
namespace MetadataClient
{
    public class MetadataEventStreamJobFacts
    {
        [Fact]
        public void AddPackageAssertions()
        {
            // Arrange
            var packageAssertions = new List<PackageAssertionSet>();
            var packageOwnerAssertions = new List<PackageOwnerAssertion>();

            var p1 = new PackageAssertionSet("A", "1.0.0", true, null, true, DateTime.MinValue, DateTime.MinValue);
            packageAssertions.Add(p1);

            var o1 = new PackageOwnerAssertion("A", "1.0.0", "user1", true);
            packageOwnerAssertions.Add(o1);

            var expectedAssertionsArray = JArray.Parse(@"[
  {
    'packageId': 'A',
    'version': '1.0.0',
    'exists': true,
    'nupkg': 'http://www.nuget.org/api/v2/package/A/1.0.0',
    'listed': true,
    'created': '0001-01-01T00:00:00',
    'published': '0001-01-01T00:00:00',
    'lastEdited' : null,
    'owners': [
      {
        'username': 'user1',
        'exists': true
      }
    ]
  }
]");

            // Act
            var actualAssertionsArray = MetadataEventStreamJob.GetJArrayAssertions(packageAssertions, packageOwnerAssertions, @"http://www.nuget.org/api/v2/package/{0}/{1}");

            // Arrange
            Assert.Equal(expectedAssertionsArray, actualAssertionsArray);
        }

        [Fact]
        public void EditPackageAssertions()
        {
            // Arrange
            var packageAssertions = new List<PackageAssertionSet>();
            var packageOwnerAssertions = new List<PackageOwnerAssertion>();

            var p1 = new PackageAssertionSet("A", "1.0.0", true, null, true, DateTime.MinValue, DateTime.MinValue);
            packageAssertions.Add(p1);

            var expectedAssertionsArray = JArray.Parse(@"[
  {
    'packageId': 'A',
    'version': '1.0.0',
    'exists': true,
    'nupkg': 'http://www.nuget.org/api/v2/package/A/1.0.0',
    'listed': true,
    'created': '0001-01-01T00:00:00',
    'published': '0001-01-01T00:00:00',
    'lastEdited' : null,
  }
]");

            // Act
            var actualAssertionsArray = MetadataEventStreamJob.GetJArrayAssertions(packageAssertions, packageOwnerAssertions, @"http://www.nuget.org/api/v2/package/{0}/{1}");

            // Arrange
            Assert.Equal(expectedAssertionsArray, actualAssertionsArray);
        }

        [Fact]
        public void DeletePackageAssertions()
        {
            // Arrange
            var packageAssertions = new List<PackageAssertionSet>();
            var packageOwnerAssertions = new List<PackageOwnerAssertion>();
            var p1 = new PackageAssertionSet("A", "1.0.0", false);
            packageAssertions.Add(p1);

            var o1 = new PackageOwnerAssertion("A", "1.0.0", "user1", true);
            packageOwnerAssertions.Add(o1);

            var expectedAssertionsArray = JArray.Parse(@"[
  {
    'packageId': 'A',
    'version': '1.0.0',
    'exists': false,
    'owners': [
      {
        'username': 'user1',
        'exists': true
      }
    ]
  }
]");

            // Act
            var actualAssertionsArray = MetadataEventStreamJob.GetJArrayAssertions(packageAssertions, packageOwnerAssertions, null);

            // Arrange
            Assert.Equal(expectedAssertionsArray, actualAssertionsArray);
        }

        [Fact]
        public void DeletePackageWithOwnerAssertions()
        {
            // Arrange
            var packageAssertions = new List<PackageAssertionSet>();
            var packageOwnerAssertions = new List<PackageOwnerAssertion>();
            var p1 = new PackageAssertionSet("A", "1.0.0", false);
            packageAssertions.Add(p1);

            var expectedAssertionsArray = JArray.Parse(@"[
  {
    'packageId': 'A',
    'version': '1.0.0',
    'exists': false
  }
]");

            // Act
            var actualAssertionsArray = MetadataEventStreamJob.GetJArrayAssertions(packageAssertions, packageOwnerAssertions, null);

            // Arrange
            Assert.Equal(expectedAssertionsArray, actualAssertionsArray);
        }

        [Fact]
        public void AddOwnerAssertions()
        {
            // Arrange
            var packageAssertions = new List<PackageAssertionSet>();
            var packageOwnerAssertions = new List<PackageOwnerAssertion>();
            var o1 = new PackageOwnerAssertion("A", "1.0.0", "user1", true);
            packageOwnerAssertions.Add(o1);
            var expectedAssertionsArray = JArray.Parse(@"[
  {
    'packageId': 'A',
    'exists': true,
    'owners': [
      {
        'username': 'user1',
        'exists': true
      },
    ]
  }
]");
            // Act
            var actualAssertionsArray = MetadataEventStreamJob.GetJArrayAssertions(packageAssertions, packageOwnerAssertions, null);

            // Arrange
            Assert.Equal(expectedAssertionsArray, actualAssertionsArray);
        }

        [Fact]
        public void RemoveOwnerAssertions()
        {
            // Arrange
            var packageAssertions = new List<PackageAssertionSet>();
            var packageOwnerAssertions = new List<PackageOwnerAssertion>();
            var o1 = new PackageOwnerAssertion("A", "1.0.0", "user1", false);
            packageOwnerAssertions.Add(o1);
            var expectedAssertionsArray = JArray.Parse(@"[
  {
    'packageId': 'A',
    'owners': [
      {
        'username': 'user1',
        'exists': false
      },
    ]
  }
]");

            // Act
            var actualAssertionsArray = MetadataEventStreamJob.GetJArrayAssertions(packageAssertions, packageOwnerAssertions, null);

            // Arrange
            Assert.Equal(expectedAssertionsArray, actualAssertionsArray);
        }

        [Fact]
        public void AddRemoveOwnerAssertions()
        {
            // Arrange
            var packageAssertions = new List<PackageAssertionSet>();
            var packageOwnerAssertions = new List<PackageOwnerAssertion>();
            var o1 = new PackageOwnerAssertion("A", "1.0.0", "user1", true);
            var o2 = new PackageOwnerAssertion("A", "1.0.0", "user2", false);
            packageOwnerAssertions.Add(o1);
            packageOwnerAssertions.Add(o2);
            var expectedAssertionsArray = JArray.Parse(@"[
  {
    'packageId': 'A',
    'exists': true,
    'owners': [
      {
        'username': 'user1',
        'exists': true
      },
      {
        'username': 'user2',
        'exists': false
      }
    ]
  }
]");
            // Act
            var actualAssertionsArray = MetadataEventStreamJob.GetJArrayAssertions(packageAssertions, packageOwnerAssertions, null);

            // Arrange
            Assert.Equal(expectedAssertionsArray, actualAssertionsArray);
        }

        [Fact]
        public void RenameOwnerAssertions()
        {
            // Arrange
            var packageAssertions = new List<PackageAssertionSet>();
            var packageOwnerAssertions = new List<PackageOwnerAssertion>();
            var o1 = new PackageOwnerAssertion("A", "1.0.0", "user1", false);
            var o2 = new PackageOwnerAssertion("A", "1.0.0", "newuser1", true);
            packageOwnerAssertions.Add(o1);
            packageOwnerAssertions.Add(o2);

            var expectedAssertionsArray = JArray.Parse(@"[
  {
    'packageId': 'A',
    'exists': true,
    'owners': [
      {
        'username': 'user1',
        'exists': false
      },
      {
        'username': 'newuser1',
        'exists': true
      }
    ]
  }
]");
            // Act
            var actualAssertionsArray = MetadataEventStreamJob.GetJArrayAssertions(packageAssertions, packageOwnerAssertions, null);

            // Arrange
            Assert.Equal(expectedAssertionsArray, actualAssertionsArray);
        }

        [Fact]
        public void AddOwnerAssertionsForSamePackageOfDifferentVersions()
        {
            // Arrange
            var packageAssertions = new List<PackageAssertionSet>();
            var packageOwnerAssertions = new List<PackageOwnerAssertion>();
            var o1 = new PackageOwnerAssertion("A", "1.0.0", "user1", true);
            var o2 = new PackageOwnerAssertion("A", "2.0.0", "user1", true);
            packageOwnerAssertions.Add(o1);
            packageOwnerAssertions.Add(o2);

            var expectedAssertionsArray = JArray.Parse(@"[
  {
    'packageId': 'A',
    'exists': true,
    'owners': [
      {
        'username': 'user1',
        'exists': true
      }
    ]
  }
]");
            // Act
            var actualAssertionsArray = MetadataEventStreamJob.GetJArrayAssertions(packageAssertions, packageOwnerAssertions, null);

            // Arrange
            Assert.Equal(expectedAssertionsArray, actualAssertionsArray);
        }

        [Fact]
        public void GetNupkgArgumentNull()
        {
            // Arrange, Act & Assert
            Assert.Throws<ArgumentNullException>(() => { MetadataEventStreamJob.GetNupkgUrl(nupkgUrlFormat: null, packageId: "A", version: "1.0.0"); });
        }

        [Fact]
        public void TestGetJObjectFirstOne()
        {
            var indexJSON = (JObject)MetadataEventStreamJob.EmptyIndexJSON.DeepClone();
            var jArrayAssertions = new JArray();
            var timeStamp = new DateTime(2014, 4, 21, 12, 30, 30, 500);
            var expectedJObject = JObject.Parse(@"{
  'timestamp': '2014-04-21T12:30:30.5',
  'older': null,
  'newer': null,
  'assertions': []
}");

            // Act
            var actualJObject = MetadataEventStreamJob.GetJObject(jArrayAssertions, timeStamp, indexJSON);

            // Assert
            Assert.Equal(expectedJObject, actualJObject);
        }

        [Fact]
        public void TestGetJObjectSecondOneOrHigher()
        {
            var indexJSON = JObject.Parse(@"{
  'lastupdated': '2014-04-21T12:30:30.5',
  'oldest': '2014/04/21/12-30-30-500Z.json',
  'newest': '2014/04/21/12-30-30-500Z.json'
}");
            var jArrayAssertions = new JArray();
            var timeStamp = new DateTime(2014, 4, 21, 12, 35, 30, 800);
            var expectedJObject = JObject.Parse(@"{
  'timestamp': '2014-04-21T12:35:30.8',
  'older': '../../../2014/04/21/12-30-30-500Z.json',
  'newer': null,
  'assertions': []
}");

            // Act
            var actualJObject = MetadataEventStreamJob.GetJObject(jArrayAssertions, timeStamp, indexJSON);

            // Assert
            Assert.Equal(expectedJObject, actualJObject);
        }

        [Fact]
        public void TestIndexJSONFirstTime()
        {
            // Arrange
            var indexJSON = (JObject)MetadataEventStreamJob.EmptyIndexJSON.DeepClone();
            var jArrayAssertions = new JArray();
            var timeStamp = new DateTime(2014, 4, 21, 12, 30, 30, 500);
            var expectedJObject = JObject.Parse(@"{
  'lastupdated': '2014-04-21T12:30:30.5',
  'oldest': '2014/04/21/12-30-30-500Z.json',
  'newest': '2014/04/21/12-30-30-500Z.json'
}");

            // Act
            var jObject = MetadataEventStreamJob.GetJObject(jArrayAssertions, timeStamp, indexJSON);

            // Act
            MetadataEventStreamJob.DumpJSON(jObject, MetadataEventStreamJob.GetBlobName(timeStamp), timeStamp, indexJSON, null, null, false).Wait();

            // Assert
            Assert.Equal(expectedJObject, indexJSON);
        }

        [Fact]
        public void TestIndexJSONSecondTimeOrLater()
        {
            // Arrange
            var indexJSON = JObject.Parse(@"{
  'lastupdated': '2014-04-21T12:30:30.5',
  'oldest': '2014/04/21/12-30-30-500Z.json',
  'newest': '2014/04/21/12-30-30-500Z.json'
}");

            var jArrayAssertions = new JArray();
            var timeStamp = new DateTime(2014, 4, 21, 12, 35, 30, 800);
            var expectedJObject = JObject.Parse(@"{
  'lastupdated': '2014-04-21T12:35:30.8',
  'oldest': '2014/04/21/12-30-30-500Z.json',
  'newest': '2014/04/21/12-35-30-800Z.json'
}");

            // Act
            var jObject = MetadataEventStreamJob.GetJObject(jArrayAssertions, timeStamp, indexJSON);

            // Act
            MetadataEventStreamJob.DumpJSON(jObject, MetadataEventStreamJob.GetBlobName(timeStamp), timeStamp, indexJSON, null, null, false).Wait();

            // Assert
            Assert.Equal(expectedJObject, indexJSON);
        }
    }
}
