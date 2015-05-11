// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
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
        public Exception Error { get; private set; }

        public WorkServiceStatus(RunnerStatus runnerStatus, Guid currentInvocationId, Guid lastInvocationId, JobDescription currentJob, JobDescription lastJob, Exception error)
        {
            RunnerStatus = runnerStatus;
            CurrentInvocationId = currentInvocationId;
            LastInvocationId = lastInvocationId;
            CurrentJob = currentJob;
            LastJob = lastJob;
            Error = error;
        }
    }

    public enum RunnerStatus
    {
        Working,
        Dequeuing,
        Sleeping,
        Dispatching,
        Stopping,
        Error
    }
}
