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
using NuGet.Services.Storage;
using System.Reactive.Subjects;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Sinks;
using System.Reactive;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;

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
            var eventSource = job.GetEventSource();
            if (eventSource == null)
            {
                InvocationEventSource.Log.NoEventSource(jobdef.Name);
            }
            else
            {
                _listener.EnableEvents(eventSource, EventLevel.Informational);
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

        private string _blobName;

        private CloudBlockBlob _logBlob;
        private JsonEventTextFormatter _formatter;

        public StorageHub Storage { get; private set; }

        public BlobInvocationLogCapture(InvocationState invocation, StorageHub storage)
            : base(invocation)
        {
            Storage = storage;

            _blobName = invocation.Id.ToString("N") + ".json";
            _logBlob = Storage.Primary.Blobs.GetBlob(WorkService.InvocationLogsContainerBaseName, "invocations/" + _blobName);
            _formatter = new JsonEventTextFormatter(EventTextFormatting.Indented, dateTimeFormat: "O");
        }

        public override async Task Start()
        {
            await base.Start();
            
            _eventSubscription = this.Buffer(TimeSpan.FromSeconds(5), 10)
                .Select(events => Observable.FromAsync(() => FlushEvents(events)))
                .Concat()
                .Subscribe(); // Side-effects are all that matter here
        }

        private async Task<Unit> FlushEvents(IList<EventEntry> events)
        {
            // Build the new entries
            StringBuilder newContent = new StringBuilder();
            using (StringWriter writer = new StringWriter(newContent))
            {
                foreach (var evt in events)
                {
                    _formatter.WriteEvent(evt, writer);
                }
            }

            int tries = 1;
            bool conflict = true;
            while(tries <= 3 && conflict) {
                conflict = false;
                // We're doing optimistic concurrency here, so we just do an upload If-Match at the end 
                // using the ETag from the first result.
                // There should only be one worker running a particular invocation, so this should work
                // but if not, we just retry later

                // Download the current text
                // We could do Exists, then DownloadText, but this has two advantages:
                //  1. Single operation to download the current text and check if it exists
                //  2. Avoids a race between Exists and DownloadText
                StringBuilder content;
                try
                {
                    content = new StringBuilder(await _logBlob.DownloadTextAsync());
                }
                catch (StorageException ex)
                {
                    if(ex.RequestInformation != null && 
                        ex.RequestInformation.ExtendedErrorInformation != null &&
                        ex.RequestInformation.ExtendedErrorInformation.ErrorCode == BlobErrorCodeStrings.BlobNotFound)
                    {
                        // Blob just didn't exist, so we're going to create it
                        content = new StringBuilder();
                    }
                    else
                    {
                        throw;
                    }
                }

                // Build the new content
                string finalContent = content.Append(newContent.ToString()).ToString();

                // Try to upload it
                try
                {
                    await _logBlob.UploadTextAsync(
                        finalContent, 
                        Encoding.UTF8, 
                        AccessCondition.GenerateIfMatchCondition(_logBlob.Properties.ETag), 
                        new BlobRequestOptions(), 
                        new OperationContext());
                }
                catch (StorageException ex)
                {
                    if (ex.RequestInformation.HttpStatusCode == 409)
                    {
                        // Conflict, just try again
                        conflict = true;
                        tries++;
                    }
                    else
                    {
                        throw;
                    }
                }
            }
            
            // Check if we succeeded
            if (conflict)
            {
                // We failed!
                InvocationEventSource.Log.FailedToUploadEvents(events.Count, tries);
            }
            else
            {
                // We succeeded!
                InvocationEventSource.Log.UploadedEvents(events.Count, tries);
            }
            
            // Bogus return value, we don't care :).
            return Unit.Default;
        }

        public override Task<Uri> End()
        {
            // Disconnect the listener
            _eventSubscription.Dispose();

            return Task.FromResult(_logBlob.Uri);
        }
    }
}
