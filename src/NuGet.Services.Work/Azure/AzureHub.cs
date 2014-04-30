using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using NuGet.Services.Configuration;

namespace NuGet.Services.Work.Azure
{
    public class AzureHub
    {
        private static readonly Regex SubjectNameMatcher = new Regex(@"O\s*=\s*(?<name>[^,]+)\[(?<id>[^\]]+)\]");
        
        public X509Certificate2 ManagementCertificate { get; private set; }
        public string SubscriptionId { get; private set; }
        public string SubscriptionName { get; private set; }

        public AzureHub(ConfigurationHub config)
        {
            ManagementCertificate = FindCert(config.GetSetting("Azure.ManagementCertificateThumbprint"));
            if (ManagementCertificate == null)
            {
                throw new ConfigurationException(Strings.AzureHub_MissingCertificate);
            }
            LoadSubscriptionIdentity(config);
        }

        public SubscriptionCloudCredentials GetCredentials()
        {
            AzureHubEventSource.Log.UsingCredentials(SubscriptionName, SubscriptionId, ManagementCertificate.Thumbprint);
            return new CertificateCloudCredentials(SubscriptionId, ManagementCertificate);
        }

        private void LoadSubscriptionIdentity(ConfigurationHub config)
        {
            // Check config first
            string subId = config.GetSetting("Azure.SubscriptionId");
            string subName = config.GetSetting("Azure.SubscriptionName");
            if (String.IsNullOrEmpty(subId) && !String.IsNullOrEmpty(subName))
            {
                throw new ConfigurationException(String.Format(CultureInfo.InvariantCulture,
                    Strings.AzureHub_MissingSubscriptionConfigSetting,
                    "Azure.SubscriptionName",
                    "Azure.SubscriptionId"));
            }
            else if (!String.IsNullOrEmpty(subId) && String.IsNullOrEmpty(subName))
            {
                throw new ConfigurationException(String.Format(CultureInfo.InvariantCulture,
                    Strings.AzureHub_MissingSubscriptionConfigSetting,
                    "Azure.SubscriptionId",
                    "Azure.SubscriptionName"));
            }
            else if (!String.IsNullOrEmpty(subId) && !String.IsNullOrEmpty(subName))
            {
                SubscriptionId = subId;
                SubscriptionName = subName;
            }
            else
            {
                // Parse it from the cert
                var match = SubjectNameMatcher.Match(ManagementCertificate.Subject);
                if (!match.Success)
                {
                    throw new ConfigurationException(Strings.AzureHub_MissingSubscription);
                }
                SubscriptionId = match.Groups["id"].Value;
                SubscriptionName = match.Groups["name"].Value;
            }

            Debug.Assert(!String.IsNullOrEmpty(SubscriptionId) && !String.IsNullOrEmpty(SubscriptionName));
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
                AzureHubEventSource.Log.NoMatch(storeLocation.ToString(), String.IsNullOrEmpty(thumbprint) ? Strings.AzureHub_NullThumbprint : thumbprint);
                return null;
            }
            // One candidate? Return it.
            else if (candidates.Count == 1)
            {
                AzureHubEventSource.Log.SingleMatch(storeLocation.ToString(), candidates[0].Thumbprint, candidates[0].Subject);
                return candidates[0];
            }
            // Multiple candidates? Return the azure management certificate
            else
            {
                var match = candidates
                    .Where(c => c.Subject.Contains("OU=azure-management"))
                    .FirstOrDefault();
                if (match == null)
                {
                    AzureHubEventSource.Log.NoMatch(storeLocation.ToString(), String.IsNullOrEmpty(thumbprint) ? Strings.AzureHub_NullThumbprint : thumbprint);
                }
                else
                {
                    AzureHubEventSource.Log.MultipleMatches(storeLocation.ToString(), match.Thumbprint, match.Subject);
                }
                return match;
            }
        }
    }
}
