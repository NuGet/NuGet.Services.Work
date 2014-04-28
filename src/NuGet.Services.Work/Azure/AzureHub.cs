using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using NuGet.Services.Configuration;

namespace NuGet.Services.Work.Azure
{
    public class AzureHub
    {
        private Lazy<X509Certificate2> _managementCertificate;

        public X509Certificate2 ManagementCertificate { get { return _managementCertificate.Value; } }
        public string SubscriptionId { get; private set; }
        
        public AzureHub(ConfigurationHub config)
        {
            _managementCertificate = new Lazy<X509Certificate2>(() => FindCert(config.GetSetting("Azure.ManagementCertThumbprint")));
            SubscriptionId = config.GetSetting("Azure.SubscriptionId");
        }

        public SubscriptionCloudCredentials GetCredentials()
        {
            if (ManagementCertificate == null)
            {
                throw new ConfigurationException(Strings.AzureHub_MissingCertificate);
            }
            else if (String.IsNullOrEmpty(SubscriptionId))
            {
                throw new ConfigurationException(Strings.AzureHub_MissingSubscription);
            }

            return new CertificateCloudCredentials(SubscriptionId, ManagementCertificate);
        }

        public SubscriptionCloudCredentials TryGetCredentials()
        {
            if (ManagementCertificate == null || String.IsNullOrEmpty(SubscriptionId))
            {
                return null;
            }

            return new CertificateCloudCredentials(SubscriptionId, ManagementCertificate);
        }

        private X509Certificate2 FindCert(string thumbprint)
        {
            return
                FindCert(thumbprint, StoreLocation.LocalMachine) ??
                FindCert(thumbprint, StoreLocation.CurrentUser);
        }

        private X509Certificate2 FindCert(string thumbprint, StoreLocation storeLocation)
        {
            X509Store store;
            try
            {
                store = new X509Store(StoreName.My, storeLocation);
                store.Open(OpenFlags.ReadOnly);
            }
            catch (Exception ex)
            {
                // Error opeing store, log it and return
                AzureHubEventSource.Log.ErrorOpeningStore(storeLocation.ToString(), StoreName.My.ToString(), ex.ToString());
                return null;
            }

            var candidates = (String.IsNullOrEmpty(thumbprint) ?
                store.Certificates.Find(X509FindType.FindByTimeValid, DateTime.Now, validOnly: false) :
                store.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, validOnly: false)).OfType<X509Certificate2>().ToList();

            // No candidates? Return null.
            if (candidates.Count == 0)
            {
                return null;
            }
            // One candidate? Return it.
            else if (candidates.Count == 1)
            {
                return candidates[0];
            }
            // Multiple candidates? Return the azure management certificate
            else
            {
                return candidates
                    .Where(c => c.Subject.Contains("OU=azure-management"))
                    .FirstOrDefault();
            }
        }
    }
}
