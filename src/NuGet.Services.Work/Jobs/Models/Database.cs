// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Management.Sql.Models;

namespace NuGet.Services.Work.Jobs.Models
{
    public class SqlDatabase
    {
        public string name { get; set; }
        public int database_id { get; set; }
        public DateTime create_date { get; set; }
        public DatabaseState state { get; set; }

        public DatabaseBackup<SqlDatabase> GetBackupMetadata()
        {
            return DatabaseBackup<SqlDatabase>.Create(this);
        }

        public override string ToString()
        {
            return String.Format("Name: {0}, Created Date: {1}, State: {2}", name, create_date, state);
        }
    }

    public class DatabaseBackup<TDb> {
        private const string BackupTimestampFormat = "yyyyMMMdd_HHmm";
        private static readonly Regex BackupNameParser = new Regex(@"(?<prefix>[^_]*)_(?<timestamp>\d{4}[A-Z]{3}\d{2}_\d{4})Z", RegexOptions.IgnoreCase);
        private const string BackupNameFormat = "{0}_{1:" + BackupTimestampFormat + "}Z";

        public TDb Db { get; private set; }
        public string Prefix { get; private set; }
        public DateTimeOffset Timestamp { get; private set; }

        public DatabaseBackup(TDb db, string prefix, DateTimeOffset timestamp)
        {
            Db = db;
            Prefix = prefix;
            Timestamp = timestamp;
        }

        internal static DatabaseBackup<SqlDatabase> Create(SqlDatabase db)
        {
            var match = BackupNameParser.Match(db.name);
            if (match.Success)
            {
                DateTimeOffset timestamp = DateTimeOffset.ParseExact(
                    match.Groups["timestamp"].Value,
                    BackupTimestampFormat,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal);
                return new DatabaseBackup<SqlDatabase>(db, match.Groups["prefix"].Value, timestamp);
            }
            return null;
        }

        internal static DatabaseBackup<Database> Create(Database azureDb)
        {
            var match = BackupNameParser.Match(azureDb.Name);
            if (match.Success)
            {
                DateTimeOffset timestamp = DateTimeOffset.ParseExact(
                    match.Groups["timestamp"].Value,
                    BackupTimestampFormat,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal);
                return new DatabaseBackup<Database>(azureDb, match.Groups["prefix"].Value, timestamp);
            }
            return null;
        }

        public static string GetName(string prefix, DateTimeOffset timestamp)
        {
            return String.Format(BackupNameFormat, prefix, timestamp);
        }

        public override bool Equals(object obj)
        {
            DatabaseBackup<TDb> other = obj as DatabaseBackup<TDb>;
            return other != null && 
                Equals(other.Db, Db);
        }

        public override int GetHashCode()
        {
            return Db.GetHashCode();
        }
    }

    public enum DatabaseState : byte
    {
        ONLINE = 0,
        RESTORING = 1,
        RECOVERING = 2,
        RECOVERY_PENDING = 3,
        SUSPECT = 4,
        EMERGENCY = 5,
        OFFLINE = 6,
        COPYING = 7
    }
}
