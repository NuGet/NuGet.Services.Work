// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Jobs.Models;

namespace NuGet.Services.Work.Jobs
{
    public abstract class DatabaseJobHandlerBase<T> : AsyncJobHandler<T>
        where T : EventSource
    {
        /// <summary>
        /// The target server, in the form of a known SQL Server (primary, warehouse, etc.)
        /// </summary>
        public KnownSqlConnection TargetServer { get; set; }

        /// <summary>
        /// The name of the database to back up
        /// </summary>
        public string TargetDatabaseName { get; set; }

        /// <summary>
        /// A connection string to the database to be backed up. The user credentials must
        /// also be valid for connecting to the master database on that server.
        /// </summary>
        public SqlConnectionStringBuilder TargetDatabaseConnection { get; set; }

        protected ConfigurationHub Config { get; set; }

        protected DatabaseJobHandlerBase(ConfigurationHub config)
        {
            Config = config;
        }

        protected virtual SqlConnectionStringBuilder GetConnectionString()
        {
            var connection = TargetDatabaseConnection;
            if (connection == null)
            {
                connection = Config.Sql
                    .GetConnectionString(TargetServer);
                if (!String.IsNullOrEmpty(TargetDatabaseName))
                {
                    connection = connection.ChangeDatabase(TargetDatabaseName);
                }
            }
            return connection;
        }

        protected internal virtual async Task<SqlDatabase> GetDatabase(SqlConnection connection, string name)
        {
            return (await connection.QueryAsync<SqlDatabase>(@"
                SELECT name, database_id, create_date, state
                FROM sys.databases
                WHERE name = @name
            ", new { name })).FirstOrDefault();
        }

        protected internal virtual Task<IEnumerable<SqlDatabase>> GetDatabases(SqlConnection connection)
        {
            return connection.QueryAsync<SqlDatabase>(@"
                SELECT name, database_id, create_date, state
                FROM sys.databases
            ");
        }

        protected internal virtual Task<IEnumerable<SqlDatabase>> GetDatabases(SqlConnection connection, DatabaseState state)
        {
            return connection.QueryAsync<SqlDatabase>(@"
                SELECT name, database_id, create_date, state
                FROM sys.databases
                WHERE state = @state
            ", new { state = (int)state });
        }
    }
}
