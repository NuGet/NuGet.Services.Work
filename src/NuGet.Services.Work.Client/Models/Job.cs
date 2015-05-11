// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NuGet.Services.Work.Models
{
    public class Job
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string Runtime { get; set; }
        public AssemblyInformation Assembly { get; set; }
        public Guid? EventProviderId { get; set; }
        public bool? Enabled { get; set; }

        public Job() { }
    }
}
