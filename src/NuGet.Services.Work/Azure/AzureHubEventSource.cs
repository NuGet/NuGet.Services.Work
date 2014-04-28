using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Azure
{
    [EventSource(Name="Outercurve-NuGet-Platform-AzureHub")]
    public class AzureHubEventSource : EventSource
    {
        public static readonly AzureHubEventSource Log = new AzureHubEventSource();
        private AzureHubEventSource() {}

        [Event(
            eventId: 1,
            Level = MessageLevel.Error,
            Message = "Error opening X.509 Store {0}/{1}: {2}")]
        public void ErrorOpeningStore(string storeLocation, string storeName, string exception) { WriteEvent(1, storeLocation, storeName, exception); }
    }
}
