// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Api
{
    internal static class Routes
    {
        public const string GetActiveInvocations = "Work-Invocations-GetActive";
        public const string GetInvocations = "Work-Invocations-GetByCriteria";
        public const string GetPurgableInvocations = "Work-Invocations-GetPurgable";
        public const string GetSingleInvocation = "Work-Invocations-GetOne";
        public const string DeleteSingleInvocation = "Work-Invocations-Delete";
        public const string DeletePurgableInvocations = "Work-Invocations-DeleteCompleted";
        public const string GetInvocationLog = "Work-Invocations-GetLog";
        public const string GetInvocationLogByCriteria = "Work-Invocations-GetLogByCriteria";
        public const string PutInvocation = "Work-Invocations-Put";
        public const string GetInvocationsByJobInstance = "Work-Invocations-GetInvocationsByJobInstance";
        public const string GetJobs = "Work-Jobs-GetAll";
        public const string GetInvocationStatistics = "Work-Invocations-GetStatistics";
        public const string GetWorkerStatistics = "Work-Worker-GetStatistics";
        public const string GetJobStatistics = "Work-Jobs-GetJobStatistics";
        public const string GetInvocationsByJob = "Work-Jobs-GetInvocationsByJob";
        public const string GetLatestForJob = "Work-Jobs-GetLatestForJob";
        public const string GetLatestLogForJob = "Work-Jobs-GetLatestLogForJob";
        public const string GetStatus = "Work-Invocations-GetStatus";
        public const string GetStatusCheck = "Work-Invocations-GetStatusCheck";
    }
}
