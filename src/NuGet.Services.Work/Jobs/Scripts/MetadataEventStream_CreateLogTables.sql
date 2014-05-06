SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- DROP Packages triggers
IF  EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[dbo].[PackagesTrigger_Delete]'))
DROP TRIGGER [dbo].[PackagesTrigger_Delete]

IF  EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[dbo].[PackagesTrigger_Insert]'))
DROP TRIGGER [dbo].[PackagesTrigger_Insert]

IF  EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[dbo].[PackagesTrigger_Update]'))
DROP TRIGGER [dbo].[PackagesTrigger_Update]

-- DROP PackageRegistrationOwners trigger
IF  EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[dbo].[PackageRegistrationOwnersTrigger_InsertUpdateDelete]'))
DROP TRIGGER [dbo].[PackageRegistrationOwnersTrigger_InsertUpdateDelete]

-- DROP Users trigger
IF  EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[dbo].[UsersTrigger_Update]'))
DROP TRIGGER [dbo].[UsersTrigger_Update]

-- CREATE Logging tables and triggers
IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogPackages]') AND type in (N'U'))
CREATE TABLE [dbo].[LogPackages]
(
	-- Primary Key
	[Key]						BIGINT		   NOT NULL PRIMARY KEY IDENTITY, 
	-- Exists
    [Exists]					BIT			   NOT NULL, 
    -- Log Processing
    [ProcessAttempts]			INT			   NOT NULL DEFAULT 0, 
    [FirstProcessingDateTime]	DATETIME 	   NULL, 
    [LastProcessingDateTime]	DATETIME 	   NULL, 
    [ProcessedDateTime]			DATETIME 	   NULL,
    -- Natural Key
    [PackageId]					NVARCHAR (128) NOT NULL, 
    [Version]					NVARCHAR (64)  NOT NULL, 
    -- Data
    [Created]                   DATETIME       NULL,
    [Published]                 DATETIME       NULL,
    [Listed]                    BIT            NULL,
    [LastEdited]                DATETIME       NULL,
)
GO

IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogPackageOwners]') AND type in (N'U'))
CREATE TABLE [dbo].[LogPackageOwners]
(
	-- Primary Key
	[Key]						BIGINT		   NOT NULL PRIMARY KEY IDENTITY, 
	-- Exists
    [Exists]BIT NOT NULL, 
    -- Log Processing
    [ProcessAttempts]			INT			   NOT NULL DEFAULT 0, 
    [FirstProcessingDateTime]	DATETIME 	   NULL, 
    [LastProcessingDateTime]	DATETIME 	   NULL, 
    [ProcessedDateTime]			DATETIME 	   NULL,
    -- Natural Key
    [Username]					NVARCHAR (64)  NOT NULL,
    [PackageId]					NVARCHAR (128) NOT NULL,
    [Version]					NVARCHAR (64)  NOT NULL
)
GO

CREATE TRIGGER dbo.UsersTrigger_Update
   ON  dbo.Users 
   AFTER UPDATE
AS
IF UPDATE(Username)
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	INSERT INTO	LogPackageOwners ([Exists], Username, PackageId, [Version])
	SELECT		[Exists]
			,	Username
			,	PackageRegistrations.Id
			,	Packages.NormalizedVersion
	FROM		(
				SELECT		[Exists] = 0
						,	deleted.[Key]
						,	deleted.Username
				FROM		deleted
				INNER JOIN	inserted
						ON	inserted.[Key] = deleted.[Key]
						AND	inserted.Username != deleted.Username
				UNION ALL
				SELECT		[Exists] = 1
						,	inserted.[Key]
						,	inserted.Username
				FROM		inserted
				INNER JOIN	deleted
						ON	deleted.[Key] = inserted.[Key]
						AND	deleted.Username != inserted.Username
				) updated
	INNER JOIN	PackageRegistrationOwners
			ON	PackageRegistrationOwners.UserKey = updated.[Key]
	INNER JOIN	PackageRegistrations
			ON	PackageRegistrations.[Key] = PackageRegistrationOwners.PackageRegistrationKey
	INNER JOIN	Packages
			ON	Packages.PackageRegistrationKey = PackageRegistrations.[Key]
END
GO

CREATE TRIGGER dbo.PackagesTrigger_Update
   ON  dbo.Packages 
   AFTER UPDATE
AS
IF UPDATE(Copyright)
OR UPDATE(Created)
OR UPDATE(Description)
OR UPDATE(HashAlgorithm)
OR UPDATE(Hash)
OR UPDATE(IconUrl)
OR UPDATE(LastUpdated)
OR UPDATE(LicenseUrl)
OR UPDATE(Published)
OR UPDATE(PackageFileSize)
OR UPDATE(ProjectUrl)
OR UPDATE(RequiresLicenseAcceptance)
OR UPDATE(Summary)
OR UPDATE(Tags)
OR UPDATE(Title)
OR UPDATE(FlattenedAuthors)
OR UPDATE(Listed)
OR UPDATE(ReleaseNotes)
OR UPDATE(Language)
OR UPDATE(MinClientVersion)
OR UPDATE(LastEdited)
OR UPDATE(HideLicenseReport)
OR UPDATE(LicenseNames)
OR UPDATE(LicenseReportUrl)
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	
	INSERT		LogPackages
	(
				[Exists]
			,	[PackageId]
			,	[Version]
			,	[Created]
			,	[Published]
			,	[Listed]
			,	[LastEdited]
	)
	SELECT		[Exists] = CASE WHEN inserted.[Key] IS NULL THEN 0 ELSE 1 END
			,	[PackageId] = PackageRegistrations.[Id]
			,	[Version] = inserted.[NormalizedVersion]
			,	[Created] = inserted.[Created]
			,	[Published] = inserted.[Published]
			,	[Listed] = inserted.[Listed]
			,	[LastEdited] = inserted.[LastEdited]
	FROM		inserted
	INNER JOIN	deleted
			ON	deleted.[Key] = inserted.[Key]
	INNER JOIN	PackageRegistrations
			ON	PackageRegistrations.[Key] = inserted.PackageRegistrationKey
END
GO

CREATE TRIGGER dbo.PackagesTrigger_Insert
   ON  dbo.Packages 
   AFTER INSERT
AS 
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	
	INSERT		LogPackages
				([Exists], PackageId, [Version], Created, Published, Listed, LastEdited)
	SELECT		[Exists] = 1
			,	PackageId = PackageRegistrations.Id
			,	[Version] = inserted.NormalizedVersion
			,   Created = inserted.Created
			,   Published = inserted.Published
			,   Listed = inserted.Listed
			,   LastEdited = inserted.LastEdited
	FROM		inserted
	INNER JOIN	PackageRegistrations
			ON	PackageRegistrations.[Key] = inserted.PackageRegistrationKey
	
	INSERT		LogPackageOwners
				([Exists], PackageId, [Version], Username)
	SELECT		[Exists] = 1
			,	PackageId = PackageRegistrations.Id
			,	[Version] = NormalizedVersion
			,	Username = Users.Username
	FROM		inserted
	INNER JOIN	PackageRegistrations
			ON	PackageRegistrations.[Key] = inserted.PackageRegistrationKey
	INNER JOIN	PackageRegistrationOwners
			ON	PackageRegistrationOwners.PackageRegistrationKey = PackageRegistrations.[Key]
	INNER JOIN	Users
			ON	Users.[Key] = PackageRegistrationOwners.UserKey
END
GO

CREATE TRIGGER dbo.PackagesTrigger_Delete
   ON  dbo.Packages 
   AFTER DELETE
AS 
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	
	INSERT		LogPackages
				([Exists], PackageId, [Version])
	SELECT		[Exists] = 0
			,	PackageId = PackageRegistrations.Id
			,	[Version] = deleted.NormalizedVersion
	FROM		deleted
	INNER JOIN	PackageRegistrations
			ON	PackageRegistrations.[Key] = deleted.PackageRegistrationKey
	
	INSERT		LogPackageOwners
				([Exists], PackageId, [Version], Username)
	SELECT		[Exists] = 0
			,	PackageId = PackageRegistrations.Id
			,	[Version] = NormalizedVersion
			,	Username = Users.Username
	FROM		deleted
	INNER JOIN	PackageRegistrations
			ON	PackageRegistrations.[Key] = deleted.PackageRegistrationKey
	INNER JOIN	PackageRegistrationOwners
			ON	PackageRegistrationOwners.PackageRegistrationKey = PackageRegistrations.[Key]
	INNER JOIN	Users
			ON	Users.[Key] = PackageRegistrationOwners.UserKey
END
GO

CREATE TRIGGER [dbo].[PackageRegistrationOwnersTrigger_InsertUpdateDelete] 
   ON  [dbo].[PackageRegistrationOwners] 
   AFTER INSERT,DELETE,UPDATE
AS 
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;	
	
	INSERT INTO	LogPackageOwners ([Exists], Username, PackageId, [Version])
	SELECT		[Exists] = 0
			,	Users.Username
			,	PackageRegistrations.Id
			,	Packages.NormalizedVersion
	FROM		deleted
	INNER JOIN	PackageRegistrations
			ON	PackageRegistrations.[Key] = deleted.PackageRegistrationKey
	INNER JOIN	Packages
			ON	Packages.PackageRegistrationKey = PackageRegistrations.[Key]
	INNER JOIN	Users
			ON	Users.[Key] = deleted.UserKey

	INSERT INTO	LogPackageOwners ([Exists], Username, PackageId, [Version])
	SELECT		[Exists] = 1
			,	Users.Username
			,	PackageRegistrations.Id
			,	Packages.NormalizedVersion
	FROM		inserted
	INNER JOIN	PackageRegistrations
			ON	PackageRegistrations.[Key] = inserted.PackageRegistrationKey
	INNER JOIN	Packages
			ON	Packages.PackageRegistrationKey = PackageRegistrations.[Key]
	INNER JOIN	Users
			ON	Users.[Key] = inserted.UserKey
END
GO