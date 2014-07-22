using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace NuGet.Services.Work
{
    public class WorkServiceStatus
    {
        public RunnerStatus RunnerStatus { get; private set; }
        public Guid CurrentInvocationId { get; private set; }
        public JobDescription CurrentJob { get; private set; }
        public Guid LastInvocationId { get; private set; }
        public JobDescription LastJob { get; private set; }

        public WorkServiceStatus(RunnerStatus runnerStatus, Guid currentInvocationId, Guid lastInvocationId, JobDescription currentJob, JobDescription lastJob)
        {
            RunnerStatus = runnerStatus;
            CurrentInvocationId = currentInvocationId;
            LastInvocationId = lastInvocationId;
            CurrentJob = currentJob;
            LastJob = lastJob;
        }
    }

    public enum RunnerStatus
    {
        Working,
        Dequeuing,
        Sleeping,
        Dispatching,
        Stopping
    }
}
