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
        private const string ContentType = "application/json";

        // Event constants
        private const string EventContext = "@context";
        private const string EventId = "@id";
        private const string EventTimeStamp = "timestamp";
        private const string EventOlder = "older";
        private const string EventNewer = "newer";
        private const string EventLastUpdated = "lastupdated";
        private const string EventOldest = "oldest";
        private const string EventNewest = "newest";
        private const string EventNull = null;
        private const string EventAssertions = "assertions";

        private const string DefaultEventStreamContainerName = "eventstream";
        /// <summary>
        /// The following cap overrides the MaxUpdateRecords and MaxPurgeRecords and never allows updates or deletes on more than 1000 records in a transaction
        /// </summary>
        private const int MaxRecordsCap = 1000;
        /// <summary>
        /// The following cap overrides the MinPurgeAge parameter and never allows purging of records within the last day
        /// </summary>
        private static readonly TimeSpan MinPurgeAgeCap = TimeSpan.FromDays(1);
        private static readonly JsonSerializerSettings DefaultJsonSerializerSettings = new JsonSerializerSettings() { ContractResolver = new CamelCasePropertyNamesContractResolver() };
        public static readonly JToken AssertionSetContext = JToken.Parse(@"{
	'@vocab' : 'http://nuget.org/schema#',
	'xsd' : 'http://www.w3.org/2001/XMLSchema#',
	'timeStamp' : { '@type' : 'xsd:dateTime' },
	'exists' :  { '@type' : 'xsd:boolean' },
	'listed' :  { '@type' : 'xsd:boolean' },
	'created' :  { '@type' : 'xsd:dateTime' },
	'published' :  { '@type' : 'xsd:dateTime' },
	'lastEdited' :  { '@type' : 'xsd:dateTime' },
	'assertions' : {
		'@container': '@set'
	}
}");

        public static readonly JObject EmptyIndexJSON = JObject.Parse(@"{
  '@context' : {
	'@vocab' : 'http://nuget.org/schema#',
	'xsd': 'http://www.w3.org/2001/XMLSchema#',
	'lastUpdated' : { '@type' : 'xsd:dateTime' }
	},
  '@id' : null,
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
        public int MaxUpdateRecords { get; set; }

        public int MaxPurgeRecords { get; set; }

        public TimeSpan MinPurgeAge { get; set; }

        private CloudBlobContainer EventStreamContainer { get; set; }

        private bool PushToCloud { get; set; }

        private bool UpdateTables { get; set; }

        public MetadataEventStreamJob(ConfigurationHub configHub) : base(configHub) { }
        protected internal override async Task<JobContinuation> Execute()
        {
            PushToCloud = true;
            UpdateTables = true;

            if (String.IsNullOrEmpty(NupkgUrlFormat))
            {
                throw new ArgumentNullException("NupkgUrlFormat");
            }

            // If MinPurgeAge is not specified, its default value TimeSpan.Zero will be less than the cap and will get overridden
            // So, no need to set a default value for it

            if (MaxPurgeRecords == 0)
            {
                MaxPurgeRecords = MaxRecordsCap;
            }

            if (MaxUpdateRecords == 0)
            {
                MaxUpdateRecords = MaxRecordsCap;
            }

            var cstr = GetConnectionString() ?? Config.Sql.GetConnectionString(KnownSqlConnection.Legacy);
            if (cstr == null)
            {
                throw new ArgumentNullException("TargetServer");
            }
            cstr.TrimNetworkProtocol();
            Log.SourceDatabase(cstr.DataSource, cstr.InitialCatalog);
            
            EventStreamStorage = EventStreamStorage ?? Config.Storage.Legacy;
            EventStreamContainer = EventStreamStorage.CreateCloudBlobClient().GetContainerReference(
                String.IsNullOrEmpty(EventStreamContainerName) ? DefaultEventStreamContainerName : EventStreamContainerName);
            Log.TargetStorageContainer(EventStreamContainer.Uri.ToString());

            if (await EventStreamContainer.CreateIfNotExistsAsync())
            {
                Log.CreatedStorageContainer();
            }

            // MinPurgeAge should be at least MinPurgeAgeCap
            MinPurgeAge = MinPurgeAge > MinPurgeAgeCap ? MinPurgeAge : MinPurgeAgeCap;
            Log.MinPurgeAge(MinPurgeAge.ToString());

            // MaxPurgeRecords should be less than MaxRecordsCap
            MaxPurgeRecords = Math.Min(MaxPurgeRecords, MaxRecordsCap);
            Log.MaxPurgeRecords(MaxPurgeRecords);

            // MaxUpdateRecords should be less than MaxRecordsCap
            MaxUpdateRecords = Math.Min(MaxUpdateRecords, MaxRecordsCap);
            Log.MaxUpdateRecords(MaxUpdateRecords);

            using (var connection = await cstr.ConnectTo())
            {
                Log.ConnectedToDatabase(connection.DataSource, connection.Database, connection.ClientConnectionId);
                Log.PurgingAssertionsStarted();
                await PurgeAssertions(connection);
                Log.PurgingAssertionsCompleted();
                Log.DetectingChangesStarted();
                await DetectChanges(connection);
                Log.DetectingChangesCompleted();
            }
            return Complete();
        }

        private async Task PurgeAssertions(SqlConnection connection)
        {
            var purgeCutoffDateTime = (DateTime.UtcNow - MinPurgeAge).Date;
            Log.PurgeCutoffDateTime(purgeCutoffDateTime);
            await PurgeAssertions(connection, purgeCutoffDateTime, MetadataEventStreamSQLQueries.CountPackageAssertionsToPurgeQuery, MetadataEventStreamSQLQueries.PurgePackageAssertionsQuery);
            await PurgeAssertions(connection, purgeCutoffDateTime, MetadataEventStreamSQLQueries.CountPackageOwnerAssertionsToPurgeQuery, MetadataEventStreamSQLQueries.PurgePackageOwnerAssertionsQuery);
        }

        private async Task PurgeAssertions(SqlConnection connection, DateTime purgeCutoffDateTime, string countQuery, string purgeQuery)
        {
            Log.QueryPurgeAssertionsCount();
            var results = await connection.QueryAsync<int>(countQuery, new { PurgeCutoffDateTime = purgeCutoffDateTime });
            var count = results.Single();
            if (count > 0)
            {
                Log.PurgeAssertionsStart(count);
                await connection.QueryAsync<int>(purgeQuery, new { MaxPurgeRecords = MaxPurgeRecords, PurgeCutoffDateTime = purgeCutoffDateTime });
                Log.PurgeAssertionsEnd();
            }
            else
            {
                Log.NoPurging();
            }
        }

        private async Task<JObject> DetectChanges(SqlConnection connection)
        {
            JObject json = null;

            Log.MultipleQueryStart();
            var results = connection.QueryMultiple(MetadataEventStreamSQLQueries.GetAssertionsQuery, new { MaxRecords = MaxUpdateRecords });
            Log.MultipleQueryEnd();

            Log.ExtractingAssertions();
            var packageAssertions = results.Read<PackageAssertionSet>().ToList();
            var packageOwnerAssertions = results.Read<PackageOwnerAssertion>().ToList();

            Log.ExtractedPackageAssertions(packageAssertions.Count);
            Log.ExtractedPackageOwnerAssertions(packageOwnerAssertions.Count);

            // Extract the assertions as JArray
            Debug.Assert(packageAssertions.Count <= MaxUpdateRecords);
            Debug.Assert(packageOwnerAssertions.Count <= MaxUpdateRecords);
            var jArrayAssertions = GetJArrayAssertions(packageAssertions, packageOwnerAssertions);

            if (jArrayAssertions.Count > 0)
            {
                var timeStamp = DateTime.UtcNow;
                Log.Timestamp(timeStamp);
                var indexJSONBlob = EventStreamContainer.GetBlockBlobReference(IndexJson);

                JObject indexJSON = await GetJSON(indexJSONBlob) ?? (JObject)EmptyIndexJSON.DeepClone();

                // Get Final JObject with timeStamp, previous, next links etc
                json = GetJObject(jArrayAssertions, timeStamp, indexJSON);

                var blobName = GetBlobName(timeStamp);
                Log.BlobName(blobName);

                // Write the blob. Update indexJSON blob and previous newest Blob
                await DumpJSON(json, blobName, timeStamp, indexJSON, indexJSONBlob);

                if (UpdateTables)
                {
                    Log.UpdateTables();
                    // Mark assertions as processed
                    await MarkAssertionsAsProcessed(connection, packageAssertions, packageOwnerAssertions);
                }
            }
            else
            {
                Log.NoAssertions();
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
        public JArray GetJArrayAssertions(IEnumerable<PackageAssertionSet> packageAssertions, IEnumerable<PackageOwnerAssertion> packageOwnerAssertions, string nupkgUrlFormat)
        {
            // For every package assertion entry, create an entry in a simple dictionary of (<packageId, packageVersion>, IAssertionSet)
            var packagesAndOwners = new Dictionary<Tuple<string, string>, IAssertionSet>();
            var ownersOnlyAssertions = new Dictionary<string, IAssertionSet>();
            foreach (var packageAssertion in packageAssertions)
            {
                var key = new Tuple<string, string>(packageAssertion.PackageId, packageAssertion.Version);
                if (packageAssertion.Exists)
                {
                    packageAssertion.Nupkg = GetJsonLdIRI(GetNupkgUrl(nupkgUrlFormat, packageAssertion.PackageId, packageAssertion.Version));
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
                    Log.PackageOwnerAssertionAlreadyExists();
                }
            }

            var assertionSets = packagesAndOwners.Values.Concat(ownersOnlyAssertions.Values);

            var json = JsonConvert.SerializeObject(assertionSets, Formatting.Indented, DefaultJsonSerializerSettings);
            return JArray.Parse(json);
        }

        private async Task<JObject> GetJSON(CloudBlockBlob blob)
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
                    Log.AzureStorageException(ex.ToString());
                }
            }
            return null;
        }

        /// <summary>
        /// Gets the final JObject given the assertions as jArray, timeStamp and indexJSON
        /// This can be tested separately to verify that the index is used and updated correctly using a mocked indexJSON JObject
        /// </summary>
        public JObject GetJObject(JArray jArrayAssertions, DateTime timeStamp, JObject indexJSON)
        {
            var json = new JObject();
            json.Add(EventContext, AssertionSetContext);
            // This will be overwritten with blob URI before pushing to cloud
            json.Add(EventId, null);

            json.Add(EventTimeStamp, timeStamp);
            if (indexJSON == null)
            {
                json.Add(EventOlder, EventNull);
            }
            else
            {
                var eventOlder = SelectJsonLdIRI(indexJSON, EventNewest);
                if (eventOlder == null)
                {
                    throw new ArgumentException("indexJSON does not have a token 'newest'");
                }
                Log.EventNewestInCurrentIndex(eventOlder.ToString());
                json.Add(EventOlder, eventOlder.Type == JTokenType.Null ? EventNull : GetJsonLdIRI(GetRelativePathToEvent(eventOlder.ToString())));
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

        public static JToken GetJsonLdIRI(string url)
        {
            if (url == null)
            {
                throw new ArgumentNullException("url");
            }
            var jsonldIRI = String.Format("{{ '@id' : '{0}' }}", url);
            var token = JToken.Parse(jsonldIRI);
            return token;
        }

        public static JToken SelectJsonLdIRI(JToken token, string propertyName)
        {
            var eventToken = token.SelectToken(propertyName);
            if (eventToken == null)
            {
                throw new ArgumentException("propertyName does not exist in token");
            }

            if (eventToken.Type == JTokenType.Null)
                return eventToken;

            var iriToken = eventToken.SelectToken("@id");
            if (iriToken == null)
            {
                throw new ArgumentException("propertyName token is not a valid json-ld IRI");
            }
            return iriToken;
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
        /// Dumps the JSON containing the assertion set onto blob storage
        /// </summary>
        public async Task DumpJSON(JObject json, string blobName, DateTime timeStamp, JObject indexJSON, CloudBlockBlob indexJSONBlob, CloudBlobContainer eventsContainer, bool pushToCloud)
        {
            if (json == null)
            {
                throw new ArgumentNullException("json");
            }

            if(blobName == null)
            {
                throw new ArgumentNullException("blobName");
            }

            if (indexJSON == null)
            {
                throw new ArgumentNullException("indexJSON");
            }

            Log.PreviousIndexJSON(indexJSON.ToString());

            string oldestBlobName = null;
            string previousNewestBlobName = null;
            oldestBlobName = SelectJsonLdIRI(indexJSON, EventOldest).ToString();
            previousNewestBlobName = SelectJsonLdIRI(indexJSON, EventNewest).ToString();

            // Update the previous newest blob
            if (String.IsNullOrEmpty(previousNewestBlobName))
            {
                if (!String.IsNullOrEmpty(oldestBlobName))
                {
                    Log.NewestEmptyWhileOldestIsNot();
                }
                // Both the oldest and newest event blob names are empty
                // Set the oldest now
                indexJSON[EventOldest] = GetJsonLdIRI(blobName);
            }

            indexJSON[EventNewest] = GetJsonLdIRI(blobName);
            indexJSON[EventLastUpdated] = timeStamp;
            indexJSON[EventId] = null;
            if (pushToCloud)
            {
                if (indexJSONBlob == null)
                {
                    throw new ArgumentNullException("indexJSONBlob");
                }
                if (eventsContainer == null)
                {
                    throw new ArgumentNullException("eventsContainer");
                }
                Log.BlobName(blobName);
                var newestBlob = eventsContainer.GetBlockBlobReference(blobName);

                // Set @id to uri of the blob
                json[EventId] = newestBlob.Uri.ToString();

                // First upload the created blob
                await Upload(newestBlob, json.ToString(), ContentType);

                if (!String.IsNullOrEmpty(previousNewestBlobName))
                {
                    CloudBlockBlob previousNewestBlob = eventsContainer.GetBlockBlobReference(previousNewestBlobName);
                    JObject previousNewestJSON = await GetJSON(previousNewestBlob);
                    if (previousNewestJSON == null)
                    {
                        throw new InvalidOperationException("Previous newest blob does not exist");
                    }

                    previousNewestJSON[EventNewer] = GetJsonLdIRI(GetRelativePathToEvent(blobName));
                    // Secondly, upload the previousNewestBlob with a 'event newer' link to the newestBlob
                    await Upload(previousNewestBlob, previousNewestJSON.ToString(), ContentType);
                    Log.PreviousNewestBlob(previousNewestBlob.Uri.ToString());
                }

                // Set @id to uri of the blob
                indexJSON[EventId] = indexJSONBlob.Uri.ToString();

                // Finally, upload the index blob
                await Upload(indexJSONBlob, indexJSON.ToString(), ContentType);

                Log.NewestBlob(newestBlob.Uri.ToString());
            }
            Log.NewIndexJSON(indexJSON.ToString());
        }

        private static async Task Upload(CloudBlockBlob blob, string content, string contentType)
        {
            blob.Properties.ContentType = contentType;
            using (var stream = new MemoryStream(Encoding.Default.GetBytes(content), false))
            {
                await blob.UploadFromStreamAsync(stream);
            }
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

        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Message = "Will look for changes in {0}/{1}")]
        public void SourceDatabase(string server, string database) { WriteEvent(1, server, database); }

		[Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Message = "Will push the events to {0} ")]
        public void TargetStorageContainer(string eventStreamContainerUri) { WriteEvent(2, eventStreamContainerUri); }
        
		[Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Message = "EventStream Container was not present. Created it")]
        public void CreatedStorageContainer() { WriteEvent(3); }
        
		[Event(
            eventId: 4,
            Level = EventLevel.Informational,
            Message = "(Capped) Min Purge Age is {0}")]
        public void MinPurgeAge(string minPurgeAge) { WriteEvent(4, minPurgeAge); }
        
		[Event(
            eventId: 34,
            Level = EventLevel.Informational,
            Message = "(Capped) Max Purge Records is {0}")]
        public void MaxPurgeRecords(int maxPurgeRecords) { WriteEvent(34, maxPurgeRecords); }
        
		[Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Message = "(Capped) Max Update Records is {0}")]
        public void MaxUpdateRecords(int maxUpdateRecords) { WriteEvent(5, maxUpdateRecords); }
        
		[Event(
            eventId: 6,
            Level = EventLevel.Informational,
            Message = "Connected to database in {0}/{1} obtained: {2}")]
        public void ConnectedToDatabase(string server, string database, Guid clientConnectionId) { WriteEvent(6, server, database, clientConnectionId); }
        
		[Event(
            eventId: 7,
            Level = EventLevel.Informational,
            Message = "Started Purging assertions")]
        public void PurgingAssertionsStarted() { WriteEvent(7); }
        
		[Event(
            eventId: 8,
            Level = EventLevel.Informational,
            Message = "Completed Purging assertions")]
        public void PurgingAssertionsCompleted() { WriteEvent(8); }
        
		[Event(
            eventId: 9,
            Level = EventLevel.Informational,
            Message = "Started Detecting changes")]
        public void DetectingChangesStarted() { WriteEvent(9); }
        
		[Event(
            eventId: 10,
            Level = EventLevel.Informational,
            Message = "Completed Detecting changes")]
        public void DetectingChangesCompleted() { WriteEvent(10); }
        
		[Event(
            eventId: 11,
            Level = EventLevel.Informational,
            Message = "Purge Cutoff DateTime : {0}")]
        public void PurgeCutoffDateTime(DateTime purgeCutoffDateTime) { WriteEvent(11, purgeCutoffDateTime); }
        
		[Event(
            eventId: 12,
            Level = EventLevel.Informational,
            Message = "Querying the number of assertions to purge...")]
        public void QueryPurgeAssertionsCount() { WriteEvent(12); }
        
		[Event(
            eventId: 13,
            Level = EventLevel.Informational,
            Message = "Started Purging {0} assertions")]
        public void PurgeAssertionsStart(int count) { WriteEvent(13, count); }
        
		[Event(
            eventId: 14,
            Level = EventLevel.Informational,
            Message = "Completed purging assertions")]
        public void PurgeAssertionsEnd() { WriteEvent(14); }
        
		[Event(
            eventId: 15,
            Level = EventLevel.Informational,
            Message = "No records to purge")]
        public void NoPurging() { WriteEvent(15); }
        
		[Event(
            eventId: 16,
            Level = EventLevel.Informational,
            Message = "Querying multiple queries...")]
        public void MultipleQueryStart() { WriteEvent(16); }
        
		[Event(
            eventId: 17,
            Level = EventLevel.Informational,
            Message = "Completed multiple queries.")]
        public void MultipleQueryEnd() { WriteEvent(17); }
        
		[Event(
            eventId: 18,
            Level = EventLevel.Informational,
            Message = "Extracting packageassertions and owner assertions...")]
        public void ExtractingAssertions() { WriteEvent(18); }
        
		[Event(
            eventId: 19,
            Level = EventLevel.Informational,
            Message = "Extracted {0} package assertions")]
        public void ExtractedPackageAssertions(int count) { WriteEvent(19, count); }
        
		[Event(
            eventId: 20,
            Level = EventLevel.Informational,
            Message = "Extracted {0} package owner assertions")]
        public void ExtractedPackageOwnerAssertions(int count) { WriteEvent(20, count); }
        
		[Event(
            eventId: 21,
            Level = EventLevel.Informational,
            Message = "Timestamp for the blob : {0}")]
        public void Timestamp(DateTime timeStamp) { WriteEvent(21, timeStamp); }
        
		[Event(
            eventId: 22,
            Level = EventLevel.Informational,
            Message = "Blobname is {0}")]
        public void BlobName(string blobName) { WriteEvent(22, blobName); }
        
		[Event(
            eventId: 23,
            Level = EventLevel.Informational,
            Message = "Updating tables to mark assertions as processed")]
        public void UpdateTables() { WriteEvent(23); }
        
		[Event(
            eventId: 24,
            Level = EventLevel.Informational,
            Message = "No Assertions to make")]
        public void NoAssertions() { WriteEvent(24); }
        
		[Event(
            eventId: 25,
            Level = EventLevel.Informational,
            Message = "PackageOwnerAssertion already exists")]
        public void PackageOwnerAssertionAlreadyExists() { WriteEvent(25); }
        
		[Event(
            eventId: 26,
            Level = EventLevel.Informational,
            Message = "Azure Storage Exception : {0}")]
        public void AzureStorageException(string message) { WriteEvent(26, message); }
        
		[Event(
            eventId: 27,
            Level = EventLevel.Informational,
            Message = "Event newest in previous index json is : {0}")]
        public void EventNewestInCurrentIndex(string eventNewest) { WriteEvent(27, eventNewest); }
        
		[Event(
            eventId: 28,
            Level = EventLevel.Informational,
            Message = "index.json PREVIOUS: '{0}' ")]
        public void PreviousIndexJSON(string previousIndexJSON) { WriteEvent(28, previousIndexJSON); }
        
		[Event(
            eventId: 29,
            Level = EventLevel.Warning,
            Message = "WARNING: OldestBlobName is not empty when newestBlobName is. Something went wrong somewhere!!!")]
        public void NewestEmptyWhileOldestIsNot() { WriteEvent(29); }
        
		[Event(
            eventId: 30,
            Level = EventLevel.Informational,
            Message = "Writing to blob {0}")]
        public void WritingToBlob(string blobName) { WriteEvent(30, blobName); }
        
		[Event(
            eventId: 31,
            Level = EventLevel.Informational,
            Message = "Previous Newest Blob: '{0}'")]
        public void PreviousNewestBlob(string previousNewestBlob) { WriteEvent(31, previousNewestBlob); }

        [Event(
            eventId: 32,
            Level = EventLevel.Informational,
            Message = "Newest Blob: '{0}'")]
        public void NewestBlob(string newestBlob) { WriteEvent(32, newestBlob); }

		[Event(
            eventId: 33,
            Level = EventLevel.Informational,
            Message = "index.json NEW: '{0}'")]
        public void NewIndexJSON(string newIndexJSON) { WriteEvent(33, newIndexJSON); }
    }
}
