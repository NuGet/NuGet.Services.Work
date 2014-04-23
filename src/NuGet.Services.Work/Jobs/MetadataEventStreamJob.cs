using Dapper;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Helpers;
using NuGet.Services.Work.Jobs.Models;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Jobs
{
    [Description("Job to create a stream of events as it happened in the Target SQL Database")]
    public class MetadataEventStreamJob : DatabaseJobHandlerBase<MetadataEventStreamEventSource>
    {
        // Formatting constants
        private readonly static string RelativeEventPathFormat = "../../../{0}";
        private readonly static string EventsPrefix = String.Empty;
        private const string DateTimeFormat = "yyyy/MM/dd/HH-mm-ss-fffZ";
        private const string EventFileNameFormat = "{0}{1}.json";

        // File constants
        private const string IndexJson = "index.json";

        // Event constants
        private const string EventTimeStamp = "timestamp";
        private const string EventOlder = "older";
        private const string EventNewer = "newer";
        private const string EventLastUpdated = "lastupdated";
        private const string EventOldest = "oldest";
        private const string EventNewest = "newest";
        private const string EventNull = null;
        private const string EventAssertions = "assertions";

        private const string DefaultEventStreamContainerName = "eventstream";
        private const int MaxRecordsCap = 1000;
        private static readonly JsonSerializerSettings DefaultJsonSerializerSettings = new JsonSerializerSettings() { ContractResolver = new CamelCasePropertyNamesContractResolver() };
        public static readonly JObject EmptyIndexJSON = JObject.Parse(@"{
  '" + EventLastUpdated + @"': '',
  '" + EventOldest + @"': null,
  '" + EventNewest + @"': null
}");

        /// <summary>
        /// Storage Account in which the event stream is stored
        /// </summary>
        public CloudStorageAccount EventStreamStorage { get; set; }

        /// <summary>
        /// Name of the Blob Container in which the event stream is stored
        /// </summary>
        public string EventStreamContainerName { get; set; }

        /// <summary>
        /// String format using which the url to the nupkg
        /// for a given packageId, and packageVersion can be created
        /// </summary>
        public string NupkgUrlFormat { get; set; }

        /// <summary>
        /// Number of records that can be pulled from any of the Log tables
        /// </summary>
        public int MaxRecords { get; set; }

        private CloudBlobContainer EventStreamContainer { get; set; }

        private bool PushToCloud { get; set; }

        private bool UpdateTables { get; set; }

        public MetadataEventStreamJob(ConfigurationHub configHub) : base(configHub) { }
        protected internal override async Task<JobContinuation> Execute()
        {
            var cstr = GetConnectionString() ?? Config.Sql.GetConnectionString(KnownSqlConnection.Legacy);
            Console.WriteLine("Will look for changes in {0}/{1} ", cstr.DataSource, cstr.InitialCatalog);
            
            EventStreamStorage = EventStreamStorage ?? Config.Storage.Legacy;
            EventStreamContainer = EventStreamStorage.CreateCloudBlobClient().GetContainerReference(
                String.IsNullOrEmpty(EventStreamContainerName) ? DefaultEventStreamContainerName : EventStreamContainerName);
            Console.WriteLine("Will push the events to {0}/{1} ", EventStreamContainer.Uri);

            if (await EventStreamContainer.CreateIfNotExistsAsync())
            {
                Console.WriteLine("EventStream Container was not present. Created it");
            }

            MaxRecords = Math.Min(MaxRecords, MaxRecordsCap);
            Console.WriteLine("(Capped) Max Records is {0}", MaxRecords);

            Console.WriteLine("Started Detecting changes");
            await DetectChanges(cstr);
            Console.WriteLine("Detected changes");

            return Complete();
        }

        private async Task<JObject> DetectChanges(SqlConnectionStringBuilder sql)
        {
            JObject json = null;

            try
            {
                using (var connection = await sql.ConnectTo())
                {
                    Console.WriteLine("Connected to database in {0}/{1} obtained: {2}", connection.DataSource, connection.Database, connection.ClientConnectionId);
                    Console.WriteLine("Querying multiple queries...");
                    var results = connection.QueryMultiple(MetadataEventStreamSQLQueries.GetAssertionsQuery, new { MaxRecords = MaxRecords });
                    Console.WriteLine("Completed multiple queries.");

                    Console.WriteLine("Extracting packageassertions and owner assertions...");
                    var packageAssertions = results.Read<PackageAssertionSet>();
                    var packageOwnerAssertions = results.Read<PackageOwnerAssertion>();

                    // Extract the assertions as JArray
                    Debug.Assert(packageAssertions.Count() <= MaxRecords);
                    Debug.Assert(packageOwnerAssertions.Count() <= MaxRecords);
                    var jArrayAssertions = GetJArrayAssertions(packageAssertions, packageOwnerAssertions);

                    if (jArrayAssertions.Count > 0)
                    {
                        var timeStamp = DateTime.UtcNow;
                        var indexJSONBlob = EventStreamContainer.GetBlockBlobReference(IndexJson);

                        JObject indexJSON = await GetJSON(indexJSONBlob) ?? (JObject)EmptyIndexJSON.DeepClone();

                        // Get Final JObject with timeStamp, previous, next links etc
                        json = GetJObject(jArrayAssertions, timeStamp, indexJSON);

                        var blobName = GetBlobName(timeStamp);

                        // Write the blob. Update indexJSON blob and previous latest Blob
                        await DumpJSON(json, blobName, timeStamp, indexJSON, indexJSONBlob);

                        if (UpdateTables)
                        {
                            // Mark assertions as processed
                            await MarkAssertionsAsProcessed(connection, packageAssertions, packageOwnerAssertions);
                        }
                        else
                        {
                            Console.WriteLine("Not Updating tables...");
                        }
                    }
                    else
                    {
                        Console.WriteLine("No Assertions to make");
                        if (UpdateTables)
                        {
                            Console.WriteLine("And, not updating tables");
                        }
                        else
                        {
                            Console.WriteLine("Not updating tables anyways...");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }

            return json;
        }

        private JArray GetJArrayAssertions(IEnumerable<PackageAssertionSet> packageAssertions, IEnumerable<PackageOwnerAssertion> packageOwnerAssertions)
        {
            return GetJArrayAssertions(packageAssertions, packageOwnerAssertions, NupkgUrlFormat);
        }

        /// <summary>
        /// Gets the assertions as JArray from the packageAssertions and packageOwnerAssertions queried from the database
        /// This can be tested separately to verify that the right jArray of assertions are created using mocked assertions
        /// </summary>
        public static JArray GetJArrayAssertions(IEnumerable<PackageAssertionSet> packageAssertions, IEnumerable<PackageOwnerAssertion> packageOwnerAssertions, string nupkgUrlFormat)
        {
            // For every package assertion entry, create an entry in a simple dictionary of (<packageId, packageVersion>, IPackageAssertion)
            var packagesAndOwners = new Dictionary<Tuple<string, string>, IAssertionSet>();
            var ownersOnlyAssertions = new Dictionary<string, IAssertionSet>();
            foreach (var packageAssertion in packageAssertions)
            {
                var key = new Tuple<string, string>(packageAssertion.PackageId, packageAssertion.Version);
                if (packageAssertion.Exists)
                {
                    packageAssertion.Nupkg = GetNupkgUrl(nupkgUrlFormat, packageAssertion.PackageId, packageAssertion.Version);
                    packagesAndOwners.Add(key, packageAssertion);
                }
                else
                {
                    // If exists is false, it means the package should be deleted
                    // Ignore all the other fields/columns
                    packagesAndOwners.Add(key, new PackageMinAssertionSet(packageAssertion.PackageId, packageAssertion.Version, false));
                }
            }

            // Now, for every packageMinOwnerAssertion created, connect the corresponding package owner assertions
            // If a packageMinOwnerAssertion is not present corresponding to the owner assertion(s),
            // they are owner only assertions. Add them to ownerAssertions list
            foreach (var packageOwnerAssertion in packageOwnerAssertions)
            {
                var key = new Tuple<string, string>(packageOwnerAssertion.PackageId, packageOwnerAssertion.Version);
                IAssertionSet assertionSet = null;
                if (!packagesAndOwners.TryGetValue(key, out assertionSet))
                {
                    var ownerKey = key.Item1;
                    if (!ownersOnlyAssertions.TryGetValue(ownerKey, out assertionSet))
                    {
                        assertionSet = ownersOnlyAssertions[ownerKey] = new PackageOwnerAssertionSet(packageOwnerAssertion.PackageId);
                    }
                }
                if (assertionSet.Owners == null)
                {
                    assertionSet.Owners = new HashSet<OwnerAssertion>();
                }

                if (!assertionSet.Owners.Add(packageOwnerAssertion))
                {
                    Console.WriteLine("PackageOwnerAssertion already exists");
                }
            }

            var assertionSets = packagesAndOwners.Values.Concat(ownersOnlyAssertions.Values);

            var json = JsonConvert.SerializeObject(assertionSets, Formatting.Indented, DefaultJsonSerializerSettings);
            return JArray.Parse(json);
        }

        private static async Task<JObject> GetJSON(CloudBlockBlob blob)
        {
            if (await blob.ExistsAsync())
            {
                try
                {
                    var json = await blob.DownloadTextAsync();
                    return JObject.Parse(json);
                }
                catch (StorageException ex)
                {
                    Console.WriteLine("Azure Storage Exception : " + ex.ToString());
                }
            }
            return null;
        }

        /// <summary>
        /// Gets the final JObject given the assertions as jArray, timeStamp and indexJSON
        /// This can be tested separately to verify that the index is used and updated correctly using a mocked indexJSON JObject
        /// </summary>
        public static JObject GetJObject(JArray jArrayAssertions, DateTime timeStamp, JObject indexJSON)
        {
            var json = new JObject();
            json.Add(EventTimeStamp, timeStamp);
            if (indexJSON == null)
            {
                json.Add(EventOlder, EventNull);
            }
            else
            {
                var eventOlder = indexJSON.SelectToken(EventNewest);
                if (eventOlder == null)
                {
                    throw new ArgumentException("indexJSON does not have a token 'newest'");
                }
                Console.WriteLine("Event newest in empty index json is :" + eventOlder.ToString());
                json.Add(EventOlder, eventOlder.Type == JTokenType.Null ? EventNull : GetRelativePathToEvent(eventOlder.ToString()));
            }
            json.Add(EventNewer, EventNull);
            json.Add(EventAssertions, jArrayAssertions);
            return json;
        }

        /// <summary>
        /// Gets the blob name of the assertion set based on a given timeStamp
        /// </summary>
        public static string GetBlobName(DateTime timeStamp)
        {
            return String.Format(EventFileNameFormat, EventsPrefix, timeStamp.ToString(DateTimeFormat));
        }

        /// <summary>
        /// Gets the nupkg url for a given packageId and version using the nupkgUrlFormat
        /// </summary>
        public static string GetNupkgUrl(string nupkgUrlFormat, string packageId, string version)
        {
            if (nupkgUrlFormat == null)
            {
                throw new ArgumentNullException("nupkgUrlFormat");
            }
            return String.Format(CultureInfo.InvariantCulture, nupkgUrlFormat, packageId, version);
        }

        private static string GetRelativePathToEvent(string eventName)
        {
            return String.Format(RelativeEventPathFormat, eventName);
        }

        private async Task DumpJSON(JObject json, string blobName, DateTime timeStamp, JObject indexJSON, CloudBlockBlob indexJSONBlob)
        {
            await DumpJSON(json, blobName, timeStamp, indexJSON, indexJSONBlob, EventStreamContainer, PushToCloud);
        }

        /// <summary>
        /// This function simply dumps the json onto console and to the blob if applicable
        /// </summary>
        public static async Task DumpJSON(JObject json, string blobName, DateTime timeStamp, JObject indexJSON, CloudBlockBlob indexJSONBlob, CloudBlobContainer eventsContainer, bool pushToCloud)
        {
            if (json == null)
            {
                throw new ArgumentNullException("json");
            }

            if (indexJSON == null)
            {
                throw new ArgumentNullException("indexJSON");
            }

            Console.WriteLine("BlobName: {0}\n", blobName);

            Console.WriteLine("index.json PREVIOUS: \n" + indexJSON.ToString());

            string oldestBlobName = null;
            string previousLatestBlobName = null;
            oldestBlobName = indexJSON.SelectToken(EventOldest).ToString();
            previousLatestBlobName = indexJSON.SelectToken(EventNewest).ToString();

            // Update the previous latest block
            if (String.IsNullOrEmpty(previousLatestBlobName))
            {
                if (!String.IsNullOrEmpty(oldestBlobName))
                {
                    Console.WriteLine("WARNING: OldestBlobName is not empty when newestBlobName is. Something went wrong somewhere!!!");
                }
                // Both the oldest and newest event blob names are empty
                // Set the oldest now
                indexJSON[EventOldest] = blobName;
            }

            // TODO: Should we store the URL instead?
            indexJSON[EventNewest] = blobName;
            indexJSON[EventLastUpdated] = timeStamp;
            if (pushToCloud)
            {
                if (eventsContainer == null)
                {
                    throw new ArgumentNullException("eventsContainer");
                }
                Console.WriteLine("Dumping to {0}", blobName);
                var latestBlob = eventsContainer.GetBlockBlobReference(blobName);

                // First upload the created block
                using (var stream = new MemoryStream(Encoding.Default.GetBytes(json.ToString()), false))
                {
                    await latestBlob.UploadFromStreamAsync(stream);
                }

                if (!String.IsNullOrEmpty(previousLatestBlobName))
                {
                    CloudBlockBlob previousLatestBlob = eventsContainer.GetBlockBlobReference(previousLatestBlobName);
                    JObject previousLatestJSON = await GetJSON(previousLatestBlob);
                    if (previousLatestJSON == null)
                    {
                        throw new InvalidOperationException("Previous latest blob does not exist");
                    }

                    previousLatestJSON[EventNewer] = GetRelativePathToEvent(blobName);
                    // Finally, upload the index block
                    using (var stream = new MemoryStream(Encoding.Default.GetBytes(previousLatestJSON.ToString()), false))
                    {
                        await previousLatestBlob.UploadFromStreamAsync(stream);
                    }
                    Console.WriteLine("Previous Latest Blob: \n" + previousLatestJSON.ToString());
                }

                // Finally, upload the index block
                using (var stream = new MemoryStream(Encoding.Default.GetBytes(indexJSON.ToString()), false))
                {
                    await indexJSONBlob.UploadFromStreamAsync(stream);
                }
            }
            else
            {
                Console.WriteLine("Not Dumping to cloud...\n");
            }
            Console.WriteLine(json);
            Console.WriteLine("index.json NEW: \n" + indexJSON.ToString());
        }

        private static async Task MarkAssertionsAsProcessed(SqlConnection connection, IEnumerable<PackageAssertionSet> packageAssertions,
            IEnumerable<PackageOwnerAssertion> packageOwnerAssertions)
        {
            var packageAssertionKeys = (from packageAssertion in packageAssertions
                                        select packageAssertion.Key).ToList();

            var packageOwnerAssertionKeys = (from packageOwnerAssertion in packageOwnerAssertions
                                             select packageOwnerAssertion.Key).ToList();

            await connection.QueryAsync<int>(MetadataEventStreamSQLQueries.ProcessAssertionsQuery,
                new { packageAssertionKeys = packageAssertionKeys, packageOwnerAssertionKeys = packageOwnerAssertionKeys });
        }
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-MetadataEventStream")]
    public class MetadataEventStreamEventSource : EventSource
    {
        public static readonly MetadataEventStreamEventSource Log = new MetadataEventStreamEventSource();
        private MetadataEventStreamEventSource() { }
    }
}
