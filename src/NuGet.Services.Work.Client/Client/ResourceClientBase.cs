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
    public abstract class ResourceClientBase
    {
        protected HttpClient Client { get; private set; }

        protected ResourceClientBase(HttpClient client)
        {
            Client = client;
        }

        protected async Task<ServiceResponse<T>> Get<T>(string url, Dictionary<string, string> queryString = null)
        {
            return await Client.GetAsync(await FormatQueryString(url, queryString)).AsServiceResponse<T>();
        }

        protected async Task<string> FormatQueryString(string url, Dictionary<string, string> queryString = null)
        {
            if (queryString == null)
            {
                return url;
            }
            FormUrlEncodedContent content = new FormUrlEncodedContent(queryString.Where(pair => pair.Value != null));
            return queryString.Count > 0 ?
                (url + "?" + (await content.ReadAsStringAsync())) :
                url;
        }

        protected Task<ServiceResponse<IEnumerable<Invocation>>> GetRange(string url, DateTimeOffset? start, DateTimeOffset? end, int? limit)
        {
            return Get<IEnumerable<Invocation>>(url, new Dictionary<string, string>() 
            {
                {"start", start == null ? null : start.Value.ToString("O")},
                {"end", end == null ? null : end.Value.ToString("O")},
                {"limit", limit == null ? null : limit.Value.ToString()}
            });
        }
    }
}
