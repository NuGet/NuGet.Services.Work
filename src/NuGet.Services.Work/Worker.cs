// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NuGet.Services.Work
{
    /// <summary>
    /// Represents a single worker thread on a WorkService instance
    /// </summary>
    public class Worker
    {
        private Task _task;

        public int Id { get; private set; }
        public JobRunner Runner { get; private set; }
        public Worker(int id, JobRunner runner)
        {
            Id = id;
            Runner = runner;
        }

        public Task StartAndRun(CancellationToken cancelToken)
        {
            _task = Task.Run(
                () => Runner.Run(cancelToken),
                cancellationToken: cancelToken);
            return _task;
        }

        public Task<object> GetCurrentStatus()
        {
            return Runner.GetCurrentStatus();
        }
    }
}
