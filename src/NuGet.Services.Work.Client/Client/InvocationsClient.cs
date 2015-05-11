// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using NuGet.Services.Client;
using NuGet.Services.Models;
using NuGet.Services.Work.Models;

namespace NuGet.Services.Work.Client
{
    public class InvocationsClient : ResourceClientBase
    {
        public InvocationsClient(HttpClient client) : base(client) {}

        public Task<ServiceResponse<Invocation>> Put(InvocationRequest request)
        {
            return Client.PutAsync(
                "work/invocations",
                new ObjectContent<InvocationRequest>(
                    request,
                    JsonFormat.Formatter))
                .AsServiceResponse<Invocation>();
        }

        public Task<ServiceResponse> GetLog(string id)
        {
            return Client.GetAsync(
                "work/invocations/" + id.ToLowerInvariant() + "/log")
                .AsServiceResponse();
        }

        public Task<ServiceResponse<IEnumerable<Invocation>>> GetByInstance(string jobInstanceName, DateTimeOffset? start, DateTimeOffset? end, int? limit)
        {
            return GetRange("work/invocations/instances/" + jobInstanceName, start, end, limit);
        }

        public Task<ServiceResponse<IEnumerable<Invocation>>> GetStatus()
        {
            return Get<IEnumerable<Invocation>>("work/invocations/status");
        }

        public Task<ServiceResponse<IEnumerable<Invocation>>> Get(InvocationListCriteria criteria)
        {
            return GetInvocations(criteria, limit: null);
        }

        public Task<ServiceResponse<IEnumerable<Invocation>>> Get(InvocationListCriteria criteria, int limit)
        {
            return GetInvocations(criteria, limit);
        }

        public Task<ServiceResponse<IEnumerable<Invocation>>> GetPurgable(DateTimeOffset? before)
        {
            return Get<IEnumerable<Invocation>>("work/invocations/purgable", new Dictionary<string, string>() {
                {"before", before == null ? null : before.Value.ToString("O") }
            });
        }

        public async Task<ServiceResponse> Purge(string id)
        {
            return await Client.DeleteAsync(
                await FormatQueryString(
                    "work/invocations/" + id.ToLowerInvariant()))
                .AsServiceResponse();
                
        }

        public async Task<ServiceResponse<IEnumerable<Invocation>>> Purge(DateTimeOffset? before)
        {
            return await Client.DeleteAsync(
                await FormatQueryString(
                    "work/invocations/purgable",
                    new Dictionary<string,string>() {
                        {"before", before == null ? null : before.Value.ToString("O")}
                    }))
                .AsServiceResponse<IEnumerable<Invocation>>();
        }

        public Task<ServiceResponse<Invocation>> Get(string id)
        {
            return Get<Invocation>("work/invocations/" + id);
        }

        public Task<ServiceResponse<InvocationStatistics>> GetStatistics()
        {
            return Get<InvocationStatistics>("work/invocations/stats");
        }

        private Task<ServiceResponse<IEnumerable<Invocation>>> GetInvocations(InvocationListCriteria criteria, int? limit)
        {
            return Get<IEnumerable<Invocation>>("work/invocations/" + criteria.ToString().ToLowerInvariant(), new Dictionary<string, string>()
            {
                {"limit", limit == null ? null : limit.Value.ToString()}
            });
        }
    }
}
