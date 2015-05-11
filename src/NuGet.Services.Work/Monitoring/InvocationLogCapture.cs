// Copyright (c) .NET Foundation. All rights reserved.
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
using Microsoft.WindowsAzure.Storage.Blob;
using System.Reactive.Subjects;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Sinks;
using System.Threading;

namespace NuGet.Services.Work.Monitoring
{
    public class InvocationLogCapture : IObservable<EventEntry>
    {
        private ObservableEventListener _listener;
        private IObservable<EventEntry> _eventStream;

        public InvocationState Invocation { get; private set; }

        public InvocationLogCapture(InvocationState invocation)
        {
            Invocation = invocation;

            // Set up an event stream
            _listener = new ObservableEventListener();
            _eventStream = from events in _listener
                           where InvocationContext.GetCurrentInvocationId() == Invocation.Id
                           select events;
        }

        public virtual Task Start()
        {
            _listener.EnableEvents(InvocationEventSource.Log, EventLevel.Informational);
            return Task.FromResult(0);
        }

        public virtual Task<Uri> End()
        {
            return Task.FromResult<Uri>(null);
        }

        public virtual void SetJob(JobDescription jobdef, JobHandlerBase job)
        {
            var sourceReferences = job.GetEventSources() ?? Enumerable.Empty<EventSourceReference>();
            if (!sourceReferences.Any())
            {
                InvocationEventSource.Log.NoEventSource(jobdef.Name);
            }
            else
            {
                foreach (var sourceReference in sourceReferences)
                {
                    _listener.EnableEvents(sourceReference.Source, sourceReference.Level);
                }
            }
        }

        public IDisposable Subscribe(IObserver<EventEntry> observer)
        {
            return _eventStream.Subscribe(observer);
        }
    }

    public class BlobInvocationLogCapture : InvocationLogCapture
    {
        private IDisposable _eventSubscription;
        
        private readonly string _tempDirectory;
        private string _tempFile;
        private string _blobName;
        private CloudBlockBlob _targetBlob;
        private Subject<Unit> _flushBuffer = new Subject<Unit>();

        public CloudBlobContainer LogContainer { get; private set; }

        public BlobInvocationLogCapture(InvocationState invocation, CloudBlobContainer logContainer)
            : base(invocation)
        {
            LogContainer = logContainer;

            _tempDirectory = Path.Combine(Path.GetTempPath(), "InvocationLogs");
            _blobName = invocation.Id.ToString("N") + ".json";
        }

        public override async Task Start()
        {
            await base.Start();

            // Calculate paths
            if (!Directory.Exists(_tempDirectory))
            {
                Directory.CreateDirectory(_tempDirectory);
            }

            // Generate an entirely unique file name
            string fileName = Invocation.Id.ToString("N") + "_" + Guid.NewGuid().ToString("N") + ".json";
            _tempFile = Path.Combine(_tempDirectory, fileName);
            if (File.Exists(_tempFile))
            {
                File.Delete(_tempFile);
            }

            // Locate the log blob
            _targetBlob = LogContainer.GetBlockBlobReference("invocations/" + _blobName);

            // Fetch the current logs if this is a continuation, we'll append to them during the invocation
            if (Invocation.IsContinuation && await _targetBlob.ExistsAsync())
            {
                await _targetBlob.DownloadToFileAsync(_tempFile, FileMode.Create);
            }

            // Capture the events into a JSON file and a plain text file
            var formatter = new JsonEventTextFormatter(EventTextFormatting.Indented, dateTimeFormat: "O");
            _eventSubscription = this.Buffer(() =>
                Observable.Amb(
                    _flushBuffer, 
                    Observable.Timer(TimeSpan.FromSeconds(5)).Select(x => Unit.Instance))
                    .Take(1))
                .Subscribe(
                onNext: evts =>
                {
                    // Dump to the temp file
                    using (var writer = new StreamWriter(new FileStream(_tempFile, FileMode.Append, FileAccess.Write)))
                    {
                        foreach (var evt in evts)
                        {
                            formatter.WriteEvent(evt, writer);
                        }
                    }

                    // Upload the temp file
                    UploadLog().Wait();
                });
        }

        private async Task UploadLog()
        {
            if (File.Exists(_tempFile))
            {
                // Upload the file to blob storage
                await LogContainer.CreateIfNotExistsAsync();
                await _targetBlob.UploadFromFileAsync(_tempFile, FileMode.Open);
            }
        }

        public override async Task<Uri> End()
        {
            // Flush the buffer
            _flushBuffer.OnNext(Unit.Instance);

            // Disconnect the listener and stop the timer
            _eventSubscription.Dispose();

            await UploadLog();

            // Delete the temp files
            File.Delete(_tempFile);

            return _targetBlob.Uri;
        }
    }
}
