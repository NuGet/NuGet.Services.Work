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
            Level = EventLevel.Error,
            Message = "Error opening X.509 Store {0}/{1}: {2}")]
        public void ErrorOpeningStore(string storeLocation, string storeName, string exception) { WriteEvent(1, storeLocation, storeName, exception); }

        [Event(
            eventId: 2,
            Level = EventLevel.Error,
            Message = "No Azure Management Certificates found in {0} store. (Search Thumbprint: {1})")]
        public void NoMatch(string storeLocation, string thumbprint) { WriteEvent(2, thumbprint); }

        [Event(
            eventId: 3,
            Level = EventLevel.Error,
            Message = "One matching certificate found in {0} store. Thumbprint: {1}, Subject: {2}")]
        public void SingleMatch(string storeLocation, string thumbprint, string subject) { WriteEvent(3, thumbprint, subject); }

        [Event(
            eventId: 4,
            Level = EventLevel.Error,
            Message = "Multiple matching certificate found in {0} store. Selecting Thumbprint: {1}, Subject: {2}")]
        public void MultipleMatches(string storeLocation, string thumbprint, string subject) { WriteEvent(4, thumbprint, subject); }

        [Event(
            eventId: 5,
            Level = EventLevel.Informational,
            Message = "Using Azure Subscription {0}[{1}]. Management Cert: {2}")]
        public void UsingCredentials(string subName, string subId, string thumbprint) { WriteEvent(5, subName, subId, thumbprint); }
    }
}
