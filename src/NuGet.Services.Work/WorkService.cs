﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Reactive.Linq;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Formatters;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Sinks;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Reactive.Subjects;
using System.Reactive.Disposables;
using Microsoft.WindowsAzure.Storage.Blob;
using NuGet.Services.Work.Monitoring;
using System.Net;
using NuGet.Services.Work.Configuration;
using Autofac.Core;
using Autofac;
using NuGet.Services.ServiceModel;
using NuGet.Services.Work.Api.Models;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Models;
using System.Threading;
using System.Diagnostics;
using Autofac.Features.ResolveAnything;
using NuGet.Services.Http;
using Microsoft.Owin;
using System.Web.Http.Routing;
using NuGet.Services.Work.Azure;

namespace NuGet.Services.Work
{
    public class WorkService : NuGetApiService
    {
        internal const string InvocationLogsContainerBaseName = "ng-work-invocations";
        public static readonly int DefaultWorkersPerCore = 2;
        private static readonly PathString _basePath = new PathString("/work");

        private List<ILifetimeScope> _scopes = new List<ILifetimeScope>();

        public override PathString BasePath
        {
            get { return _basePath; }
        }

        public IEnumerable<JobDescription> Jobs { get; private set; }
        public IEnumerable<Worker> Workers { get; private set; }
        public int? MaxWorkers { get; protected set; }
        public int? WorkersPerCore { get; protected set; }

        protected InvocationQueue Queue { get; set; }

        public WorkService(ServiceName name, ServiceHost host)
            : base(name, host)
        {
            var workConfig = host.Config.GetSection<WorkConfiguration>();

            MaxWorkers = MaxWorkers ?? workConfig.MaxWorkers;
            WorkersPerCore = WorkersPerCore ?? (workConfig.WorkersPerCore ?? DefaultWorkersPerCore);
        }

        protected override async Task<bool> OnStart()
        {
            try
            {
                // Discover jobs
                DiscoverJobs();

                // Determine how many workers to create
                Debug.Assert(WorkersPerCore.HasValue); // Constructor should have set a default value
                int workerCount = WorkersPerCore.Value * Environment.ProcessorCount;
                if (MaxWorkers != null)
                {
                    workerCount = Math.Min(workerCount, MaxWorkers.Value);
                }

                // Create the workers
                Workers = Enumerable.Range(0, workerCount).Select(CreateWorker);

                return await base.OnStart();
            }
            catch (Exception ex)
            {
                // Exceptions that escape to this level are fatal
                WorkServiceEventSource.Log.StartupError(ex);
                return false;
            }
        }

        protected override Task OnRun()
        {
            return Task.WhenAll(Workers.Select(w => w.StartAndRun(Host.ShutdownToken)));
        }

        protected override void OnShutdown()
        {
            foreach (var scope in _scopes)
            {
                scope.Dispose();
            }
        }

        private Worker CreateWorker(int id)
        {
            // Create a scope for this worker
            var scope = Container.BeginLifetimeScope(builder =>
            {
                // Register components
                builder.RegisterModule(
                    new JobComponentsModule(
                        ServiceName.ToString() + "#" + id.ToString(),
                        Queue));

                // Register the worker
                builder
                    .RegisterType<Worker>()
                    .WithParameter(new NamedParameter("id", id))
                    .AsSelf()
                    .SingleInstance();
            });
            _scopes.Add(scope);
            var worker = scope.Resolve<Worker>();
            worker.Runner.Heartbeat += (sender, args) =>
            {
                Heartbeat();
            };
            return worker;
        }

        private void DiscoverJobs()
        {
            Jobs = Container.Resolve<IEnumerable<JobDescription>>();

            foreach (var job in Jobs)
            {
                // Record the discovery in the trace
                WorkServiceEventSource.Log.JobDiscovered(job);
            }
        }

        public override void RegisterComponents(ContainerBuilder builder)
        {
            base.RegisterComponents(builder);

            var jobdefs = GetAllAvailableJobs();
            builder.RegisterInstance(jobdefs).As<IEnumerable<JobDescription>>();

            // Register an invocation queue for the API to use. The workers will register
            // their own in a sub scope
            if (Queue == null)
            {
                builder
                    .RegisterType<InvocationQueue>()
                    .AsSelf()
                    .UsingConstructor(
                        typeof(Clock),
                        typeof(string),
                        typeof(ConfigurationHub))
                    .WithParameter(
                        new NamedParameter("instanceName", ServiceName.ToString() + "#api"));
            }
            else
            {
                builder.RegisterInstance(Queue).As<InvocationQueue>();
            }
        }

        public static IEnumerable<JobDescription> GetAllAvailableJobs()
        {
            var jobdefs = typeof(WorkService)
                   .Assembly
                   .GetExportedTypes()
                   .Where(t => !t.IsAbstract && typeof(JobHandlerBase).IsAssignableFrom(t))
                   .Select(t => JobDescription.Create(t))
                   .Where(d => d != null);
            return jobdefs;
        }

        public override async Task<object> GetCurrentStatus()
        {
            return (await Task.WhenAll(Workers.Select(async w => Tuple.Create(w.Id, await w.GetCurrentStatus())))).ToDictionary(
                t => ServiceName.ToString() + "#" + t.Item1.ToString(),
                t => t.Item2);
        }

        protected override void ConfigureAttributeRouting(DefaultInlineConstraintResolver resolver)
        {
            base.ConfigureAttributeRouting(resolver);
            resolver.ConstraintMap.Add("invocationListCriteria", typeof(EnumConstraint<InvocationListCriteria>));
        }

        public override IEnumerable<EventSource> GetEventSources()
        {
            yield return WorkServiceEventSource.Log;
            yield return InvocationEventSource.Log;
        }
    }
}
