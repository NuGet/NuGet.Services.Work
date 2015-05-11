// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using NuGet.Services.Work.Helpers;
using NuGet.Services.Work.Monitoring;

namespace NuGet.Services.Work
{
    public abstract class JobHandlerBase<TEventSource> : JobHandlerBase
        where TEventSource : EventSource
    {
        private TEventSource _log = EventSourceInstanceManager.Get<TEventSource>();
        private IList<EventSourceReference> _additionalSources = new List<EventSourceReference>();

        public TEventSource Log { get { return _log; } }

        public override IEnumerable<EventSourceReference> GetEventSources()
        {
            yield return new EventSourceReference(Log, EventLevel.LogAlways);
            foreach (var source in _additionalSources)
            {
                yield return source;
            }
        }

        protected void AddEventSource(EventSource log)
        {
            AddEventSource(log, EventLevel.LogAlways);
        }

        protected void AddEventSource(EventSource log, EventLevel level)
        {
            _additionalSources.Add(new EventSourceReference(log, level));
        }
    }

    public abstract class JobHandler<TEventSource> : JobHandlerBase<TEventSource>
        where TEventSource : EventSource
    {
        protected internal override async Task<InvocationResult> Invoke()
        {
            try
            {
                await Execute();
                return InvocationResult.Completed();
            }
            catch (Exception ex)
            {
                return InvocationResult.Faulted(ex);
            }
        }

        protected internal abstract Task Execute();
    }

    public abstract class RepeatingJobHandler<TEventSource> : JobHandlerBase<TEventSource> 
        where TEventSource: EventSource
    {
        /// <summary>
        /// The amount of time to wait before invoking again, when the job goes idle
        /// </summary>
        public abstract TimeSpan WaitPeriod { get; }

        protected internal override async Task<InvocationResult> Invoke()
        {
            // Invoke the job. When it returns, it means there's no more data to process
            // So go to sleep until the wait period elapses
            try
            {
                await Execute();
                return InvocationResult.Completed(WaitPeriod);
            }
            catch (Exception ex)
            {
                return InvocationResult.Faulted(ex, WaitPeriod);
            }
        }

        protected internal abstract Task Execute();
    }

    public interface IAsyncJob
    {
        Task<InvocationResult> InvokeContinuation(InvocationContext context);
    }

    public abstract class AsyncJobHandler<TEventSource> : JobHandlerBase<TEventSource>, IAsyncJob
        where TEventSource : EventSource
    {
        protected internal override Task<InvocationResult> Invoke()
        {
            return InvokeCore(() => Execute());
        }

        public Task<InvocationResult> InvokeContinuation(InvocationContext context)
        {
            InvocationResult result = BindContext(context);
            if (result != null)
            {
                return Task.FromResult(result);
            }

            return InvokeCore(() => Resume());
        }

        protected internal abstract Task<JobContinuation> Execute();
        protected internal virtual Task<JobContinuation> Resume() { return Task.FromResult(Complete()); }

        // Helper methods for returning "Suspend" and "Complete" results (even though Complete is just null)
        protected JobContinuation Suspend(TimeSpan waitPeriod, object parameters)
        {
            return Suspend(
                waitPeriod,
                (from p in parameters.GetType().GetTypeInfo().DeclaredProperties
                 select new { p.Name, Value = p.GetValue(parameters) })
                 .ToDictionary(a => a.Name, a => a.Value == null ? null : a.Value.ToString()));
        }

        protected JobContinuation Suspend(TimeSpan waitPeriod, Dictionary<string, string> parameters)
        {
            // TODO: Using a [ContinuationState] or similar attribute, allow the job author
            //  to just mark properties that they want carried over to the continuation.
            //  Then just build the 'parameters' dictionary by reading those values.
            return new JobContinuation(waitPeriod, parameters);
        }

        protected JobContinuation Complete()
        {
            return null;
        }

        private async Task<InvocationResult> InvokeCore(Func<Task<JobContinuation>> invoker)
        {

            try
            {
                var continuation = await invoker();
                if (continuation != null)
                {
                    return InvocationResult.Suspended(continuation);
                }
                else
                {
                    return InvocationResult.Completed();
                }
            }
            catch (Exception ex)
            {
                return InvocationResult.Faulted(ex);
            }
        }
    }
}
