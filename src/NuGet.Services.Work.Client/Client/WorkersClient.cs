// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using NuGet.Services.Client;
using NuGet.Services.Work.Models;

namespace NuGet.Services.Work.Client
{
    public class WorkersClient
    {
        private HttpClient _client;

        public WorkersClient(HttpClient client)
        {
            _client = client;
        }

        public Task<ServiceResponse<IEnumerable<InstanceStatistics>>> GetStatistics()
        {
            return _client.GetAsync("work/workers/stats").AsServiceResponse<IEnumerable<InstanceStatistics>>();
        }
    }
}
