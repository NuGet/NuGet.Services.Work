// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using NuGet.Services.Configuration;

namespace NuGet.Services.Work.Jobs
{
    [Description("Rebuilds the SQL Indexes in the Gallery database")]
    public class RebuildGalleryIndexesJob : JobHandler<RebuildGalleryIndexesJobEventSource>
    {
        /// <summary>
        /// Gets or sets a connection string to the database containing package data.
        /// </summary>
        public SqlConnectionStringBuilder GalleryConnection { get; set; }

        /// <summary>
        /// Gets or sets the command timeout (in seconds)
        /// </summary>
        public int CommandTimeout { get; set; }

        protected ConfigurationHub Config { get; set; }

        public RebuildGalleryIndexesJob(ConfigurationHub config)
        {
            Config = config;
        }

        protected internal override async Task Execute()
        {
            // Load default data if not provided
            GalleryConnection = GalleryConnection ?? Config.Sql.GetConnectionString(KnownSqlConnection.Legacy);

            using (var connection = await GalleryConnection.ConnectTo())
            {
                Log.RebuildingIndexes(GalleryConnection.DataSource, GalleryConnection.InitialCatalog);
                if (!WhatIf)
                {
                    SqlCommand rebuild = connection.CreateCommand();
                    rebuild.CommandText = RebuildIndexesSql;
                    rebuild.CommandTimeout = CommandTimeout > 0 ? CommandTimeout :
                        60 * // seconds
                        60 * // minutes
                        4;   // hours

                    await Extend(TimeSpan.FromSeconds(rebuild.CommandTimeout));
                    await rebuild.ExecuteNonQueryAsync();
                }
                Log.RebuiltIndexes(GalleryConnection.DataSource, GalleryConnection.InitialCatalog);
            }
        }

        const string RebuildIndexesSql = @"
            ALTER INDEX [IX_Package_IsLatestStable] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_PackageKey] ON [PackageHistories] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_PackageRegistration_Id] ON [PackageRegistrations] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_Package_IsLatest] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_Packages_IsLatestStable] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_UserKey] ON [PackageHistories] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_PackageRegistrationKey] ON [CuratedPackages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_UserKey] ON [Credentials] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_Package_Listed] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_Package_Version] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_Credentials_Type_Value] ON [Credentials] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_UserKey] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_PackageRegistration_Id_Key] ON [PackageRegistrations] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_UsersByUsername] ON [Users] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__PackageR__9F10F88ED9F94B28] ON [PackageRegistrationOwners] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_PackageRegistrationOwners_UserKey] ON [PackageRegistrationOwners] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_CuratedFeed_PackageRegistration] ON [CuratedPackages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_PackageKey] ON [PackageLicenseReports] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_LicenseKey] ON [PackageLicenseReportLicenses] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_CuratedFeedKey] ON [CuratedPackages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_Packages_LastEdited] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_Package_Key] ON [PackageFrameworks] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__PackageR__C41E02887DE8477A] ON [PackageRegistrations] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_PackageDependencies] ON [PackageDependencies] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_dbo.Credentials] ON [Credentials] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_PackageFrameworks] ON [PackageFrameworks] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__PackageD__C41E028820A36EA6] ON [PackageDependencies] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_ELMAH_Error_App_Time_Seq] ON [ELMAH_Error] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_dbo.PackageLicenseReportLicenses] ON [PackageLicenseReportLicenses] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_ReportKey] ON [PackageLicenseReportLicenses] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_dbo.PackageLicenseReports] ON [PackageLicenseReports] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__UserRole__746466A0DA1B628B] ON [UserRoles] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_dbo.PackageLicenses] ON [PackageLicenses] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__Check2__3213663B3E27EAA9] ON [Check2] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [pk_packageregistrationkey] ON [TempUnlisted] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_PackageKey] ON [PackageEdits] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_UserKey] ON [PackageEdits] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_CuratedFeedManagers] ON [CuratedFeedManagers] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_CuratedFeedKey] ON [CuratedFeedManagers] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_UserKey] ON [CuratedFeedManagers] REBUILD WITH (ONLINE=ON);

            /* The following indexes cannot be rebuilt because they contain LOB columns

            ALTER INDEX [IX_Package_Search] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_Packages_PackageRegistrationKey] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_PackageAuthors_PackageKey] ON [PackageAuthors] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__Users__C41E02883EBA0358] ON [Users] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_PackageEdits] ON [PackageEdits] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_PackageHistories] ON [PackageHistories] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__Packages__C41E02887160FA94] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__ImageIco__C41E028866670F0D] ON [ImageIconTransfers] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_CuratedPackages] ON [CuratedPackages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__PackageA__C41E0288B1787E6F] ON [PackageAuthors] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK____Migrat__E5D3573B66137667] ON [__MigrationHistory] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__PackageS__C41E028871DC7A1C] ON [PackageStatistics] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_ELMAH_Error] ON [ELMAH_Error] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__PackageF__C41E0288CB72753D] ON [PackageFileTransfers] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__GalleryS__C41E02881735782E] ON [GallerySettings] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__PackageO__C41E0288F196EEB8] ON [PackageOwnerRequests] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_ELMAH_Error] ON [ELMAH_Error] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_PackageHistories] ON [PackageHistories] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_PackageEdits] ON [PackageEdits] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_Packages_PackageRegistrationKey] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [IX_Package_Search] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__EmailMes__C41E02886C136C04] ON [EmailMessages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__Roles__C41E0288AF146949] ON [Roles] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK__Packages__C41E02887160FA94] ON [Packages] REBUILD WITH (ONLINE=ON);
            ALTER INDEX [PK_CuratedFeeds] ON [CuratedFeeds] REBUILD WITH (ONLINE=ON);
            */
            ";
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-RebuildGalleryIndexes")]
    public class RebuildGalleryIndexesJobEventSource : EventSource
    {
        public static readonly RebuildGalleryIndexesJobEventSource Log = new RebuildGalleryIndexesJobEventSource();
        private RebuildGalleryIndexesJobEventSource() { }

        [Event(
            eventId: 1,
            Task = Tasks.RebuildingIndexes,
            Opcode = EventOpcode.Start,
            Level = EventLevel.Informational,
            Message = "Rebuilding Indexes in {0}/{1}")]
        public void RebuildingIndexes(string server, string database) { WriteEvent(1, server, database); }

        [Event(
            eventId: 2,
            Task = Tasks.RebuildingIndexes,
            Opcode = EventOpcode.Stop,
            Level = EventLevel.Informational,
            Message = "Rebuilt Indexes in {0}/{1}")]
        public void RebuiltIndexes(string server, string database) { WriteEvent(2, server, database); }

        public static class Tasks
        {
            public const EventTask RebuildingIndexes = (EventTask)0x1;
        }
    }
}
