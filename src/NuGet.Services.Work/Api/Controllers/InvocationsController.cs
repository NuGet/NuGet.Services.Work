// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using NuGet.Services.Http;
using NuGet.Services.Work.Api.Models;
using NuGet.Services.Work.Models;
using System.Net.Http;
using NuGet.Services.Configuration;
using Microsoft.WindowsAzure.Storage.Blob;

namespace NuGet.Services.Work.Api.Controllers
{
    [RoutePrefix("invocations")]
    [Authorize(Roles = Roles.Admin)]
    public class InvocationsController : NuGetApiController
    {
        public CloudBlobContainer LogContainer { get; private set; }
        public InvocationQueue Queue { get; private set; }

        public InvocationsController(ConfigurationHub config, InvocationQueue queue)
            : this(
                config.Storage.Primary
                    .CreateCloudBlobClient()
                    .GetContainerReference(WorkService.InvocationLogsContainerBaseName), 
                queue)
        {
        }

        public InvocationsController(CloudBlobContainer logContainer, InvocationQueue queue)
        {
            LogContainer = logContainer;
            Queue = queue;
        }

        [HttpGet]
        [Route("purgable", Name = Routes.GetPurgableInvocations)]
        public async Task<IHttpActionResult> GetPurgable(DateTimeOffset? before = null)
        {
            return Content(
                HttpStatusCode.OK, 
                (await Queue.GetPurgable(before ?? DateTimeOffset.UtcNow))
                .Select(i => i.ToModel()));
        }

        [Route("purgable", Name = Routes.DeletePurgableInvocations)]
        public async Task<IHttpActionResult> DeletePurgableInvocations(DateTimeOffset? before = null)
        {
            var purged = await Queue.PurgeCompleted(before ?? DateTimeOffset.UtcNow);

            // Return the data that was purged
            return Content(HttpStatusCode.OK, purged.Select(i => i.ToModel(Url)));
        }

        [Route("{id}/log", Name = Routes.GetInvocationLog)]
        public async Task<IHttpActionResult> GetInvocationLog(Guid id)
        {
            // Locate the blob
            var blob = LogContainer.GetBlockBlobReference("invocations/" + id.ToString("N") + ".json");

            if (!await blob.ExistsAsync())
            {
                return NotFound();
            }

            return await TransferBlob(blob.Uri);
        }

        [Route("", Name = Routes.GetActiveInvocations)]
        public Task<IHttpActionResult> GetActive(int? limit = null)
        {
            return Get(InvocationListCriteria.Active, limit);
        }

        [Route("instances/{instanceName}", Name = Routes.GetInvocationsByJobInstance)]
        public async Task<IHttpActionResult> GetByInstance(string instanceName, DateTime? start = null, DateTime? end = null, int? limit = null)
        {
            return Content(HttpStatusCode.OK, (await Queue.GetByInstance(instanceName, start, end, limit)).Select(i => i.ToModel(Url)));
        }

        [Route("status", Name = Routes.GetStatus)]
        public async Task<IHttpActionResult> GetStatus()
        {
            return Content(HttpStatusCode.OK, (await Queue.GetLatestByJob()).Select(i => i.ToModel(Url)));
        }

        [Route("status/check", Name = Routes.GetStatusCheck)]
        public async Task<IHttpActionResult> GetStatusCheck()
        {
            var data = (await Queue.GetLatestByJob()).Select(i => i.ToModel(Url)).ToList();
            var status = data.Any(i => i.Result == ExecutionResult.Crashed || i.Result == ExecutionResult.Faulted) ?
                HttpStatusCode.InternalServerError :
                HttpStatusCode.OK;
            return Content(status, data);
        }

        [Route("{criteria:invocationListCriteria}", Name = Routes.GetInvocations)]
        public async Task<IHttpActionResult> Get(InvocationListCriteria criteria, int? limit = null)
        {
            if (!Enum.IsDefined(typeof(InvocationListCriteria), criteria))
            {
                return NotFound();
            }

            return Content(HttpStatusCode.OK, (await Queue.GetAll(criteria, limit)).Select(i => i.ToModel(Url)));
        }

        [Route("{id}", Name = Routes.GetSingleInvocation)]
        public async Task<IHttpActionResult> Get(Guid id)
        {
            var invocation = await Queue.Get(id);
            if (invocation == null)
            {
                return NotFound();
            }
            return Content(HttpStatusCode.OK, invocation.ToModel(Url));
        }

        [Route("", Name = Routes.PutInvocation)]
        public async Task<IHttpActionResult> Put([FromBody] InvocationRequest request)
        {
            var invocation = await Queue.Enqueue(
                request.Job, 
                request.Source ?? Constants.Source_Unknown, 
                request.Payload,
                request.VisibilityDelay ?? TimeSpan.Zero,
                request.JobInstanceName,
                request.UnlessAlreadyRunning);
            if (invocation == null)
            {
                return StatusCode(HttpStatusCode.NoContent);
            }
            else
            {
                return Content(HttpStatusCode.Created, invocation.ToModel(Url));
            }
        }

        [Route("{id}", Name = Routes.DeleteSingleInvocation)]
        public async Task Delete(Guid id)
        {
            await Queue.Purge(id);
        }

        [Route("stats", Name = Routes.GetInvocationStatistics)]
        public async Task<IHttpActionResult> GetStatistics()
        {
            var stats = await Queue.GetStatistics();
            if (stats == null)
            {
                return StatusCode(HttpStatusCode.ServiceUnavailable);
            }
            else
            {
                return Content(HttpStatusCode.OK, stats.ToInvocationModel());
            }
        }
    }
}
