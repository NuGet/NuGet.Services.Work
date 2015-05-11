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
    public class JobsClient : ResourceClientBase
    {
        public JobsClient(HttpClient client) : base(client) { }

        public Task<ServiceResponse<IEnumerable<JobStatistics>>> GetStatistics()
        {
            return Get<IEnumerable<JobStatistics>>("work/jobs/stats");
        }

        public Task<ServiceResponse<IEnumerable<Job>>> Get()
        {
            return Get<IEnumerable<Job>>("work/jobs");
        }

        public Task<ServiceResponse<IEnumerable<Invocation>>> GetByJob(string jobName, DateTimeOffset? start, DateTimeOffset? end, int? limit)
        {
            return GetRange("work/jobs/" + jobName + "/invocations", start, end, limit);
        }

        public Task<ServiceResponse<Invocation>> GetLatestInvocation(string jobName)
        {
            return Get<Invocation>("work/jobs/" + jobName + "/latest");
        }

        public Task<ServiceResponse> GetLatestInvocationLog(string jobName)
        {
            return Client.GetAsync("work/jobs/" + jobName + "/log").AsServiceResponse();
        }
    }
}
