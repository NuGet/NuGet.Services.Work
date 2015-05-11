// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work
{
    internal class Constants
    {
        public const string Source_Unknown = "Unknown";
        public const string Source_LocalJob = "LocalJob";
        public const string Source_AsyncContinuation = "AsyncContinuation";
        public const string Source_RepeatingJob = "RepeatingJob";
    }
}
