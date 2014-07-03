using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace NuGet.Services.Work.Helpers
{
    public static class PackageDeletor
    {
        public static string Normalize(string version)
        {
            SemanticVersion parsed;
            if (!SemanticVersion.TryParse(version, out parsed))
            {
                return version;
            }
            return ToNormalizedString(parsed);
        }
        public static string ToNormalizedString(SemanticVersion version)
        {
            // SemanticVersion normalizes the missing components to 0.
            return String.Format(CultureInfo.InvariantCulture,
                "{0}.{1}.{2}{3}{4}",
                version.Version.Major,
                version.Version.Minor,
                version.Version.Build,
                version.Version.Revision > 0 ? ("." + version.Version.Revision.ToString(CultureInfo.InvariantCulture)) : String.Empty,
                !String.IsNullOrEmpty(version.SpecialVersion) ? ("-" + version.SpecialVersion) : String.Empty);

        }
        public static string ToNormalizedStringSafe(SemanticVersion version)
        {
            return version != null ? ToNormalizedString(version) : String.Empty;
        }
        public static async Task<DataTable> QueryDatatable(SqlConnection connection, string query, params SqlParameter[] parameters)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = query;
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.AddRange(parameters);
                var reader = await cmd.ExecuteReaderAsync();
                DataTable table = new DataTable();
                table.Load(reader);
                return table;
            }
        }
        public static async Task DeletePackage(dynamic package, SqlConnection connection, CloudStorageAccount account)
        {
            // Capture the data from the database
            var packageRecord = await QueryDatatable(connection,
            "SELECT * FROM Packages WHERE [Key] = @key",
            new SqlParameter("@key", package.Key));
            var registrationRecord = await QueryDatatable(connection,
            "SELECT * FROM PackageRegistrations WHERE [Key] = @key",
            new SqlParameter("@key", package.PackageRegistrationKey));

            await DeletePackageData(package, connection);

            await DeletePackageBlob(package, account);
        }
        public static async Task DeleteRegistration(SqlConnection conn, string id)
        {
            // Capture the data from the database
            var registrationRecord = await QueryDatatable(conn,
            "SELECT * FROM PackageRegistrations WHERE id = @Id",
            new SqlParameter("@Id", id));

            // Delete all data
            var result = conn.Query(@"
	            BEGIN TRAN
	
	            DECLARE @actions TABLE(
		            TableName nvarchar(50),
		            Value nvarchar(MAX)
	            )
	
	            DELETE por 
	            OUTPUT 'PackageOwnerRequests' AS TableName, u.Username AS Value INTO @actions
	            FROM PackageOwnerRequests por 
	            JOIN PackageRegistrations pr ON pr.[Key] = por.PackageRegistrationKey 
	            JOIN Users u ON por.NewOwnerKey = u.[Key]
	            WHERE pr.Id = @Id
	
	            DELETE por 
	            OUTPUT 'PackageRegistrationOwners' AS TableName, u.Username AS Value INTO @actions
	            FROM PackageRegistrationOwners por 
	            JOIN PackageRegistrations pr ON pr.[Key] = por.PackageRegistrationKey 
	            JOIN Users u ON por.UserKey = u.[Key]
	            WHERE pr.Id = @Id
	
	            DELETE pr 
	            OUTPUT 'PackageRegistrations' AS TableName, deleted.Id AS Value INTO @actions
	            FROM PackageRegistrations pr 
	            WHERE pr.Id = @Id
	
	            SELECT * FROM @actions
	            COMMIT TRAN", new
                {
                    Id = id
                });
        }
        private static async Task DeletePackageData(dynamic package, SqlConnection connection)
        {
            var result = await connection.QueryAsync<int>(@"
		    BEGIN TRAN
		
		    DECLARE @actions TABLE(
		    TableName nvarchar(50),
		    Value nvarchar(MAX)
		    )
		
		    DELETE pa 
		    OUTPUT 'PackageAuthors' AS TableName, deleted.Name AS Value INTO @actions
		    FROM PackageAuthors pa 
		    JOIN Packages p ON p.[Key] = pa.PackageKey 
		    WHERE p.[Key] = @key
		
		    DELETE pd 
		    OUTPUT 
		    'PackageDependencies' AS TableName, 
		    (ISNULL(deleted.Id, '') + ' ' + ISNULL(deleted.VersionSpec, '') + ' ' + ISNULL(deleted.TargetFramework, '')) AS Value 
		    INTO @actions
		    FROM PackageDependencies pd 
		    JOIN Packages p 
		    ON p.[Key] = pd.PackageKey 
		    WHERE p.[Key] = @key
		
		    DELETE ps 
		    FROM PackageStatistics ps 
		    JOIN Packages p ON p.[Key] = ps.PackageKey 
		    WHERE p.[Key] = @key
		
		    INSERT INTO @actions
		    SELECT 'PackageStatistics' AS TableName, @@RowCount AS Value
		
		    DELETE pf 
		    FROM PackageEdits pf 
		    JOIN Packages p ON p.[Key] = pf.PackageKey 
		    WHERE p.[Key] = @key
		
		    INSERT INTO @actions
		    SELECT 'PackageEdits' AS TableName, @@RowCount AS Value
		
		    DELETE pe 
		    FROM PackageEdits pe
		    JOIN Packages p ON p.[Key] = pe.PackageKey 
		    WHERE p.[Key] = @key
		
		    INSERT INTO @actions
		    SELECT 'PackageEdits' AS TableName, @@RowCount AS Value
		
		    DELETE pf 
		    OUTPUT
		    'PackageFrameworks' AS TableName,
		    deleted.TargetFramework AS Value
		    INTO @actions
		    FROM PackageFrameworks pf 
		    JOIN Packages p ON p.[Key] = pf.Package_Key 
		    WHERE p.[Key] = @key
		
		    DELETE ph
		    OUTPUT
		    'PackageHistories' AS TableName,
		    deleted.Hash AS Value
		    INTO @actions
		    FROM PackageHistories ph 
		    JOIN Packages p ON p.[Key] = ph.PackageKey 
		    WHERE p.[Key] = @key
		
		    DELETE p 
		    OUTPUT
		    'Packages' AS TableName,
		    (pr.Id + ' ' + deleted.NormalizedVersion) AS Value
		    INTO @actions
		    FROM Packages p 
		    JOIN PackageRegistrations pr ON p.PackageRegistrationKey = pr.[Key]
		    WHERE p.[Key] = @key
		
		    SELECT * FROM @actions
		    COMMIT TRAN", new
                    {
                        key = (int)package.Key
                    });
        }
        private static async Task DeletePackageBlob(dynamic package, CloudStorageAccount account)
        {
            string id = ((string)package.Id).ToLowerInvariant();
            string version = ((string)package.Version).ToLowerInvariant();

            // Get the blob URL
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference("packages");
            var blob = container.GetBlockBlobReference(
            id + "." + version + ".nupkg");

            // Delete the blob
            await blob.DeleteIfExistsAsync(
                DeleteSnapshotsOption.IncludeSnapshots,
                AccessCondition.GenerateEmptyCondition(),
                new BlobRequestOptions(), new OperationContext());
        }
        public static async Task<IEnumerable<dynamic>> GetDeletePackages(CloudStorageAccount account, SqlConnection conn, string id, string version, bool allVersions)
        {
            // Parse the version
            if (!String.IsNullOrWhiteSpace(version))
            {
                version = Normalize(version);
            }

            var packages = await conn.QueryAsync<dynamic>(@"
	SELECT
		p.[Key],
		p.PackageRegistrationKey,
		pr.Id,
		p.NormalizedVersion AS Version, 
		p.Hash 
	FROM Packages p
	INNER JOIN PackageRegistrations pr ON p.PackageRegistrationKey = pr.[Key]
	WHERE pr.Id = @Id AND (@AllVersions = 1 OR p.NormalizedVersion = @Version)", new
                                                                                {
                                                                                    id,
                                                                                    allVersions,
                                                                                    version
                                                                                });
            return packages;
        }
    }
}
