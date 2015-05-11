// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Linq.Expressions;
using System.ComponentModel;
using Microsoft.WindowsAzure.Storage.Table;
using System.Diagnostics.Tracing;
using System.ComponentModel.DataAnnotations.Schema;
using Autofac.Builder;

namespace NuGet.Services.Work
{
    public class JobDescription
    {
        public string Name { get; private set; }
        public string Description { get; set; }
        public Guid? EventProviderId { get; set; }
        public bool? Enabled { get; set; }
        public string Runtime { get; private set; }
        public AssemblyInformation Assembly { get; private set; }

        public Type Implementation { get; private set; }
        
        [Obsolete("For serialization only")]
        public JobDescription() { }

        public JobDescription(string name, Type implementation)
            : this(name, null, null, implementation) { }

        public JobDescription(string name, string description, Guid? eventProviderId, Type implementation)
        {
            Name = name;
            Description = description;
            EventProviderId = eventProviderId;
            Implementation = implementation;
            Runtime = implementation.FullName;
            Assembly = implementation.GetAssemblyInfo();
        }

        public static JobDescription Create(Type jobType)
        {
            var attr = JobAttribute.Get(jobType);
            var descAttr = jobType.GetCustomAttribute<DescriptionAttribute>();

            return new JobDescription(
                name: attr.Name,
                description: descAttr == null ? null : descAttr.Description,
                eventProviderId: attr.EventProvider == null ? (Guid?)null : (Guid?)EventSource.GetGuid(attr.EventProvider),
                implementation: jobType);
        }
    }
}
