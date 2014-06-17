﻿using System;
using System.Reactive.Linq;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Formatters;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Sinks;

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

        public virtual Task Start() {
            _listener.EnableEvents(InvocationEventSource.Log, EventLevel.Informational);
            return Task.FromResult<object>(null);
        }

        public virtual Task<Uri> End() 
        {
            return Task.FromResult<Uri>(null);
        }

        public virtual void SetJob(JobDescription jobdef, JobHandlerBase job)
        {
            var sources = job.GetEventSources() ?? Enumerable.Empty<EventSource>();
            if (!sources.Any())
            {
                InvocationEventSource.Log.NoEventSource(jobdef.Name);
            }
            else
            {
                foreach (var source in sources)
                {
                    _listener.EnableEvents(source, EventLevel.Informational);
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
        private SinkSubscription<FlatFileSink> _eventSubscription;

        private readonly string _tempDirectory;
        private readonly CloudBlobContainer _container;
        private string _tempFile;
        private string _blobName;
        
        public BlobInvocationLogCapture(InvocationState invocation, CloudBlobContainer container)
            : base(invocation)
        {
            _container = container;

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
            
            // Fetch the current logs if this is a continuation, we'll append to them during the invocation
            if (Invocation.IsContinuation)
            {
                await _container.DownloadBlob("invocations/" + _blobName, _tempFile);
            }
            
            // Capture the events into a JSON file and a plain text file
            _eventSubscription = this.LogToFlatFile(_tempFile, new JsonEventTextFormatter(EventTextFormatting.Indented, dateTimeFormat: "O"));
        }

        public override async Task<Uri> End()
        {
            // Disconnect the listener
            _eventSubscription.Dispose();

            // Upload the file to blob storage
            var logBlob = await _container.UploadBlob("invocations/" + _blobName, _tempFile, "application/json");

            // Delete the temp files
            File.Delete(_tempFile);

            return logBlob.Uri;
        }
    }
}
