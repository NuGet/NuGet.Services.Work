// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NuGet.Services.Work.Models;

namespace NuGet.Services.Work.Api.Models
{
    public class WorkServiceModel
    {
        public IEnumerable<Job> Jobs { get; private set; }

        public WorkServiceModel() { }
        public WorkServiceModel(IEnumerable<JobDescription> jobs)
        {
            Jobs = jobs.Select(j => j.ToModel());
        }
    }
}
