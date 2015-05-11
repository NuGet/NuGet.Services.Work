// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Autofac;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using NuGet.Services.Http;
using NuGet.Services.Work.Monitoring;
using NuGet.Services.ServiceModel;
using NuGet.Services.Work.Configuration;
using NuGet.Services.Hosting.Azure;

namespace NuGet.Services.Work
{
    public class WorkWorkerRole : SingleServiceWorkerRole<WorkService> { }
}
