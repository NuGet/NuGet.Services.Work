﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using NuGet.Services.Http;
using NuGet.Services.Work.Api.Models;
using NuGet.Services.Storage;

namespace NuGet.Services.Work.Api.Controllers
{
    [RoutePrefix("jobs")]
    [Authorize(Roles = Roles.Admin)]
    public class JobsController : NuGetApiController
    {
        public StorageHub Storage { get; private set; }
        public InvocationQueue Queue { get; private set; }

        public JobsController(StorageHub storage, InvocationQueue queue)
        {
            Storage = storage;
            Queue = queue;
        }

        [Route("", Name = Routes.GetJobs)]
        public IHttpActionResult Get()
        {
            // Find the work service
            var workService = Host.GetInstance<WorkService>();
            if (workService == null)
            {
                return Content(HttpStatusCode.OK, new object[0]);
            }
            else
            {
                return Content(HttpStatusCode.OK, workService.Jobs.Select(j => j.ToModel()));
            }
        }

        [Route("stats", Name = Routes.GetJobStatistics)]
        public async Task<IHttpActionResult> GetStatistics()
        {
            var stats = await Queue.GetJobStatistics();
            return Content(HttpStatusCode.OK, stats.Select(s => s.ToJobModel()));
        }

        [Route("{jobName}/invocations", Name = Routes.GetInvocationsByJob)]
        public async Task<IHttpActionResult> GetByJob(string jobName, DateTime? start = null, DateTime? end = null, int? limit = null)
        {
            return Content(HttpStatusCode.OK, (await Queue.GetByJob(jobName, start, end, limit)).Select(i => i.ToModel(Url)));
        }

        [Route("{jobName}/latest", Name = Routes.GetLatestForJob)]
        public async Task<IHttpActionResult> GetLatestByJob(string jobName)
        {
            var invocation = await Queue.GetLatestForJob(jobName);
            if (invocation == null)
            {
                return NotFound();
            }
            return Content(HttpStatusCode.OK, invocation.ToModel(Url));
        }

        [Route("{jobName}/log", Name = Routes.GetLatestLogForJob)]
        public async Task<IHttpActionResult> GetLatestLogByJob(string jobName)
        {
            var invocation = await Queue.GetLatestForJob(jobName);
            if (invocation == null || String.IsNullOrEmpty(invocation.LogUrl))
            {
                return NotFound();
            }
            return await TransferBlob(invocation.LogUrl);
        }
    }
}
