// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using NuGet.Indexing;
using NuGet.Services.Configuration;

namespace NuGet.Services.Work.Jobs.Bases
{
    public abstract class SearchIndexJobHandlerBase<T> : JobHandler<T>
        where T : EventSource
    {
        public SqlConnectionStringBuilder PackageDatabase { get; set; }
        public CloudStorageAccount StorageAccount { get; set; }
        public string IndexContainerName { get; set; }
        public string DataContainerName { get; set; }
        public string LocalIndexFolder { get; set; }

        protected ConfigurationHub Config { get; set; }

        public SearchIndexJobHandlerBase(ConfigurationHub config)
        {
            Config = config;
        }

        protected override InvocationResult BindContext(InvocationContext context)
        {
            var result = base.BindContext(context);

            // Load default values
            PackageDatabase = PackageDatabase ?? Config.Sql.Legacy;
            StorageAccount = StorageAccount ?? Config.Storage.Primary;
            IndexContainerName = IndexContainerName ?? "ng-search";

            return result;
        }
    }
}
