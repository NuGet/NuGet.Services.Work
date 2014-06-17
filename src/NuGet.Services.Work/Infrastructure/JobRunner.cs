﻿using System;
using System.Threading;
using System.Threading.Tasks;
using NuGet.Services.Configuration;
using NuGet.Services.Work.Configuration;
using NuGet.Services.Work.Monitoring;
using NuGet.Services.Work.Models;

namespace NuGet.Services.Work
{
    public class JobRunner
    {
        public static readonly TimeSpan DefaultInvisibilityPeriod = TimeSpan.FromMinutes(30);

        private TimeSpan _pollInterval;
        private RunnerStatus _status;

        private byte[] _currentInvocationId = Guid.Empty.ToByteArray();

        protected Clock Clock { get; set; }
        protected InvocationQueue Queue { get; set; }
        protected JobDispatcher Dispatcher { get; set; }
        protected ConfigurationHub Config { get; set; }

        public RunnerStatus Status
        {
            get { return _status; }
            set { _status = value; OnHeartbeat(EventArgs.Empty); }
        }
        
        public event EventHandler Heartbeat;

        protected JobRunner(TimeSpan pollInterval)
        {
            _status = RunnerStatus.Working;
            _pollInterval = pollInterval;
        }

        public JobRunner(JobDispatcher dispatcher, InvocationQueue queue, ConfigurationHub config, Clock clock)
            : this(config.GetSection<WorkConfiguration>().PollInterval)
        {
            Dispatcher = dispatcher;
            Queue = queue;
            Clock = clock;
            Config = config;
        }

        public Task<object> GetCurrentStatus()
        {
            return Task.FromResult<object>(new WorkServiceStatus(
                _status,
                new Guid(_currentInvocationId),
                Dispatcher.GetCurrentJob()));
        }

        public virtual async Task Run(CancellationToken cancelToken)
        {
            WorkServiceEventSource.Log.DispatchLoopStarted();
            try
            {
                while (!cancelToken.IsCancellationRequested)
                {
                    InvocationState invocation = null;
                    try
                    {
                        Status = RunnerStatus.Dequeuing;
                        invocation = await Queue.Dequeue(DefaultInvisibilityPeriod, cancelToken);
                        Status = RunnerStatus.Working;
                    }
                    catch (Exception ex)
                    {
                        WorkServiceEventSource.Log.ErrorRetrievingInvocation(ex);
                    }
                    
                    // Check Cancellation
                    if (cancelToken.IsCancellationRequested)
                    {
                        break;
                    }

                    if (invocation == null)
                    {
                        Status = RunnerStatus.Sleeping;
                        WorkServiceEventSource.Log.DispatchLoopWaiting(_pollInterval);
                        await Clock.Delay(_pollInterval, cancelToken);
                        WorkServiceEventSource.Log.DispatchLoopResumed();
                        Status = RunnerStatus.Working;
                    }
                    else if (invocation.Status == InvocationStatus.Cancelled)
                    {
                        // Job was cancelled by the user, so just continue.
                        WorkServiceEventSource.Log.Cancelled(invocation);
                    }
                    else
                    {
                        Status = RunnerStatus.Dispatching;
                        Interlocked.Exchange(ref _currentInvocationId, invocation.Id.ToByteArray());
                        
                        Exception dispatchError = null;
                        try
                        {
                            await Dispatch(invocation, cancelToken);
                        }
                        catch (Exception ex)
                        {
                            dispatchError = ex;
                        }
                        if (dispatchError != null)
                        {
                            await Queue.Complete(
                                invocation,
                                ExecutionResult.Crashed,
                                dispatchError.ToString(),
                                null);
                        }
                        Interlocked.Exchange(ref _currentInvocationId, Guid.Empty.ToByteArray());
                        Status = RunnerStatus.Working;
                    }
                }
                Status = RunnerStatus.Stopping;
            }
            catch (Exception ex)
            {
                WorkServiceEventSource.Log.DispatchLoopError(ex);
                throw;
            }
            WorkServiceEventSource.Log.DispatchLoopEnded();
        }

        protected virtual void OnHeartbeat(EventArgs args)
        {
            var handler = Heartbeat;
            if (handler != null)
            {
                handler(this, args);
            }
        }

        protected internal virtual Task Dispatch(InvocationState invocation, CancellationToken cancelToken)
        {
            var container = Config.Storage.Primary.CreateCloudBlobClient().GetContainerReference(WorkService.InvocationLogsContainerName);
            var logCapture = new BlobInvocationLogCapture(invocation, container);
            return Dispatch(invocation, logCapture, cancelToken);
        }

        protected internal virtual Task Dispatch(InvocationState invocation, InvocationLogCapture capture, CancellationToken cancelToken)
        {
            return Dispatch(invocation, capture, cancelToken, includeContinuations: false);
        }

        protected internal virtual async Task Dispatch(InvocationState invocation, InvocationLogCapture capture, CancellationToken cancelToken, bool includeContinuations)
        {
            InvocationContext.SetCurrentInvocationId(invocation.Id);
            
            if (invocation.IsContinuation)
            {
                InvocationEventSource.Log.Resumed();
            }
            else
            {
                InvocationEventSource.Log.Started();
            }
            
            // Record that we are executing the job
            if (!await Queue.UpdateStatus(invocation, InvocationStatus.Executing, ExecutionResult.Incomplete))
            {
                InvocationEventSource.Log.Aborted(invocation);
                return;
            }

            // Create the request.Invocation context and start capturing the logs
            await capture.Start();
            var context = new InvocationContext(invocation, Queue, cancelToken, capture);

            InvocationResult result = null;
            try
            {
                result = await Dispatcher.Dispatch(context);

                // TODO: If response.Continuation != null, enqueue continuation
                switch (result.Result)
                {
                    case ExecutionResult.Completed:
                        InvocationEventSource.Log.Succeeded(result);
                        break;
                    case ExecutionResult.Faulted:
                        InvocationEventSource.Log.Faulted(result);
                        break;
                    case ExecutionResult.Incomplete:
                        InvocationEventSource.Log.Suspended(result);
                        break;
                    default:
                        InvocationEventSource.Log.UnknownStatus(result);
                        break;
                }

                if (invocation.NextVisibleAt < Clock.UtcNow)
                {
                    InvocationEventSource.Log.InvocationTookTooLong(invocation);
                }
            }
            catch (Exception ex)
            {
                InvocationEventSource.Log.DispatchError(ex);
                result = new InvocationResult(ExecutionResult.Crashed, ex);
            }

            // Stop capturing and set the log url
            string logUrl = null;
            if (capture != null)
            {
                var logUri = await capture.End();
                if (logUri != null)
                {
                    logUrl = logUri.AbsoluteUri;
                }
            }

            if (result.Result != ExecutionResult.Incomplete)
            {
                // If we're not suspended, the invocation has completed
                InvocationEventSource.Log.Ended();
                await Queue.Complete(invocation, result.Result, result.Exception == null ? null : result.Exception.ToString(), logUrl);

                // If we've completed and there's a repeat, queue the repeat
                if (result.RescheduleIn != null)
                {
                    // Rescheule it to run again
                    var repeat = await EnqueueRepeat(invocation, result);
                    InvocationEventSource.Log.ScheduledRepeat(invocation, repeat, result.RescheduleIn.Value);
                }
            }
            else
            {
                if (includeContinuations)
                {
                    invocation.Update(new InvocationState.InvocationRow()
                    {
                        Id = Guid.NewGuid(),
                        Version = invocation.CurrentVersion + 1,
                        Job = invocation.Job,
                        Source = invocation.Id.ToString("N"),
                        Status = (int)InvocationStatus.Suspended,
                        Result = (int)ExecutionResult.Incomplete,
                        LastDequeuedAt = invocation.LastDequeuedAt == null ? (DateTime?)null : invocation.LastDequeuedAt.Value.UtcDateTime,
                        LastSuspendedAt = DateTime.UtcNow,
                        CompletedAt = null,
                        QueuedAt = invocation.QueuedAt.UtcDateTime,
                        NextVisibleAt = invocation.NextVisibleAt.UtcDateTime + DefaultInvisibilityPeriod,
                        UpdatedAt = invocation.UpdatedAt.UtcDateTime,
                        Payload = InvocationPayloadSerializer.Serialize(result.Continuation.Parameters),
                        IsContinuation = true
                    });

                    // Run the continuation inline after waiting
                    await Task.Delay(result.Continuation.WaitPeriod);

                    await Dispatch(invocation, capture, cancelToken, includeContinuations);
                }
                else
                {
                    // Suspend the job until the continuation is ready to run
                    await Queue.Suspend(invocation, result.Continuation.Parameters, result.Continuation.WaitPeriod, logUrl);
                }
            }
        }

        private Task<InvocationState> EnqueueRepeat(InvocationState repeat, InvocationResult result)
        {
            return Queue.Enqueue(repeat.Job, Constants.Source_RepeatingJob, repeat.Payload, result.RescheduleIn.Value);
        }
    }
}
