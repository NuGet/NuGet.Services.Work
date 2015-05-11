// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NuGet.Services.Work
{
    public class JobContinuation
    {
        public TimeSpan WaitPeriod { get; private set; }
        public Dictionary<string, string> Parameters { get; private set; }

        public JobContinuation(TimeSpan waitPeriod, Dictionary<string, string> parameters)
        {
            WaitPeriod = waitPeriod;
            Parameters = parameters;
        }
    }
}
