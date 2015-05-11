// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Helpers
{
    public static class MetadataEventStreamSQLQueries
    {
        public const string LogTablesExistenceQuery = @"
SELECT      COUNT(*) FROM sys.objects
WHERE       object_id = OBJECT_ID(N'[dbo].[LogPackages]') AND type in (N'U')

SELECT      COUNT(*) FROM sys.objects
WHERE       object_id = OBJECT_ID(N'[dbo].[LogPackageOwners]') AND type in (N'U')";

        public const string GetAssertionsQuery = @"
DECLARE		@PackageAssertions TABLE
(
			[Key] int
		,	PackageId nvarchar(128)
		,	[Version] nvarchar(64)
)

DECLARE		@PackageOwnerAssertions TABLE
(
			[Key] int
		,	Username nvarchar(64)
		,	PackageId nvarchar(128)
		,	[Version] nvarchar(64)
)

DECLARE		@ProcessingDateTime datetime = GETUTCDATE()

BEGIN TRAN

UPDATE		LogPackages
SET			ProcessAttempts = ProcessAttempts + 1
		,	FirstProcessingDateTime = ISNULL(FirstProcessingDateTime, @ProcessingDateTime)
		,	LastProcessingDateTime = @ProcessingDateTime
OUTPUT		inserted.[Key]
		,	inserted.PackageId
		,	inserted.[Version]
INTO		@PackageAssertions
WHERE       [Key] IN
            (
            SELECT  TOP(@MaxRecords) [Key]
            FROM    dbo.LogPackages
            WHERE	ProcessedDateTime IS NULL
            ORDER BY [Key])

UPDATE		LogPackageOwners
SET			ProcessAttempts = ProcessAttempts + 1
		,	FirstProcessingDateTime = ISNULL(FirstProcessingDateTime, @ProcessingDateTime)
		,	LastProcessingDateTime = @ProcessingDateTime
OUTPUT		inserted.[Key]
		,	inserted.Username
		,	inserted.PackageId
		,	inserted.Version
INTO		@PackageOwnerAssertions
WHERE       [Key] IN
            (
            SELECT  TOP(@MaxRecords) [Key]
            FROM    dbo.LogPackageOwners
            WHERE	ProcessedDateTime IS NULL
            ORDER BY [Key])

UPDATE		LogPackages
SET			ProcessedDateTime = @ProcessingDateTime
WHERE		[Key] IN
            (
            SELECT      [Key]
            FROM        @PackageAssertions
            WHERE       [Key] NOT IN
                        (
                        SELECT      MaxKey = MAX([Key])
                        FROM		@PackageAssertions
                        GROUP BY	PackageId
		                        ,	[Version]))

UPDATE		LogPackageOwners
SET			ProcessedDateTime = @ProcessingDateTime
WHERE		[Key] IN
            (
            SELECT      [Key]
            FROM		@PackageOwnerAssertions
            WHERE		[Key] NOT IN
                        (
                        SELECT      MaxKey = MAX([Key])
			            FROM		@PackageOwnerAssertions
			            GROUP BY	Username
					            ,	PackageId
					            ,	[Version]))

COMMIT TRAN

SELECT		LogPackages.*
FROM		(
			SELECT		MaxKey = MAX([Key])
					,	PackageId
					,	[Version]
			FROM		@PackageAssertions
			GROUP BY	PackageId
					,	[Version]
			) PackageAssertions
INNER JOIN	LogPackages WITH (NOLOCK)
		ON	LogPackages.[Key] = PackageAssertions.MaxKey

SELECT		LogPackageOwners.*
FROM		(
			SELECT		MaxKey = MAX([Key])
					,	Username
					,	PackageId
					,	[Version]
			FROM		@PackageOwnerAssertions
			GROUP BY	Username
					,	PackageId
					,	[Version]
			) PackageOwnerAssertions
INNER JOIN	LogPackageOwners WITH (NOLOCK)
		ON	LogPackageOwners.[Key] = PackageOwnerAssertions.MaxKey";

        public const string ProcessAssertionsQuery = @"
DECLARE		@ProcessedDateTime datetime = GETUTCDATE()

UPDATE		LogPackages
SET			ProcessedDateTime = @ProcessedDateTime
WHERE		[Key] IN @packageAssertionKeys

UPDATE		LogPackageOwners
SET			ProcessedDateTime = @ProcessedDateTime
WHERE		[Key] IN @packageOwnerAssertionKeys";

        // Purge Assertions Queries

        public const string PurgePackageAssertionsQuery = @"
DELETE TOP(@MaxPurgeRecords) FROM dbo.LogPackages
WHERE ProcessedDateTime IS NOT NULL
AND ProcessedDateTime < @PurgeCutoffDateTime";

        public const string CountPackageAssertionsToPurgeQuery = @"
SELECT COUNT(*) FROM dbo.LogPackages WITH(NOLOCK)
WHERE ProcessedDateTime IS NOT NULL
AND ProcessedDateTime < @PurgeCutoffDateTime";

        public const string PurgePackageOwnerAssertionsQuery = @"
DELETE TOP(@MaxPurgeRecords) FROM dbo.LogPackageOwners
WHERE ProcessedDateTime IS NOT NULL
AND ProcessedDateTime < @PurgeCutoffDateTime";

        public const string CountPackageOwnerAssertionsToPurgeQuery = @"
SELECT COUNT(*) FROM dbo.LogPackageOwners WITH(NOLOCK)
WHERE ProcessedDateTime IS NOT NULL
AND ProcessedDateTime < @PurgeCutoffDateTime";
    }
}
