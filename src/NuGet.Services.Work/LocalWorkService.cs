// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using NuGet.Services.Configuration;
using NuGet.Services.Hosting;
using NuGet.Services.ServiceModel;
using NuGet.Services.Work.Models;
using NuGet.Services.Work.Monitoring;

namespace NuGet.Services.Work
{
    [Service("LocalWork")]
    public class LocalWorkService : WorkService
    {
        [Obsolete("Do not use, use LocalWorkService.Create instead")]
        public LocalWorkService(ServiceName name, ServiceHost host)
            : base(name, host)
        {
            Queue = InvocationQueue.Null;
        }

        public static Task<LocalWorkService> Create()
        {
            return Create(new Dictionary<string, string>());
        }

        public static async Task<LocalWorkService> Create(IDictionary<string, string> configuration)
        {
            var host = new LocalServiceHost(
                new NuGetStartOptions()
                {
                    AppDescription = new ServiceHostDescription(
                        new ServiceHostInstanceName(
                            new ServiceHostName(
                                new DatacenterName(
                                    new EnvironmentName(
                                        "nuget",
                                        "local"),
                                    0),
                                "work"),
                            0),
                        Environment.MachineName),
                    Configuration = configuration,
                    Services = new[] { "LocalWork" }
                });
            var name = new ServiceName(host.Description.InstanceName, ServiceDefinition.FromType<WorkService>().Name);
            host.Initialize();
            if (!await host.Start())
            {
                throw new InvalidOperationException(Strings.LocalWorker_FailedToStart);
            }
            return host.GetInstance<LocalWorkService>();
        }

        public IObservable<EventEntry> RunJob(string job, string payload)
        {
            var runner = new JobRunner(
                new JobDispatcher(
                    GetAllAvailableJobs(),
                    Container),
                InvocationQueue.Null,
                Container.Resolve<ConfigurationHub>(),
                Clock.RealClock);

            var invocation =
                new InvocationState(
                    new InvocationState.InvocationRow()
                    {
                        Payload = payload,
                        Status = (int)InvocationStatus.Executing,
                        Result = (int)ExecutionResult.Incomplete,
                        Source = Constants.Source_LocalJob,
                        Id = Guid.NewGuid(),
                        Job = job,
                        UpdatedBy = Environment.MachineName,
                        UpdatedAt = DateTime.UtcNow,
                        QueuedAt = DateTime.UtcNow,
                        NextVisibleAt = DateTime.UtcNow + TimeSpan.FromMinutes(5)
                    });
            return Observable.Create<EventEntry>(observer =>
            {
                var capture = new InvocationLogCapture(invocation);
                capture.Subscribe(e => observer.OnNext(e), ex => observer.OnError(ex));
                runner.Dispatch(invocation, capture, CancellationToken.None, includeContinuations: true).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        observer.OnError(t.Exception);
                    }
                    else
                    {
                        observer.OnCompleted();
                    }
                    return t;
                });
                return () => { }; // No action on unsubscribe
            });
        }
    }
}
