// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NuGet.Services.Configuration;

namespace NuGet.Services.Work.Jobs
{
    public abstract class ReportGeneratingJobBase<T> : JobHandler<T> where T : EventSource
    {
        private readonly string _defaultContainerName;

        public CloudStorageAccount Destination { get; set; }
        public string DestinationContainerName { get; set; }
        public string OutputDirectory { get; set; }

        protected ConfigurationHub Config { get; set; }
        protected CloudBlobContainer DestinationContainer { get; set; }

        protected ReportGeneratingJobBase(ConfigurationHub config, string defaultContainerName)
        {
            Config = config;
            _defaultContainerName = defaultContainerName;
        }

        protected internal override Task Execute()
        {
            LoadDefaults();

            return ExecuteCore();
        }

        protected abstract Task ExecuteCore();

        protected virtual void LoadDefaults()
        {
            Destination = Destination ?? Config.Storage.Primary;
            if (Destination != null)
            {
                DestinationContainer = Destination.CreateCloudBlobClient().GetContainerReference(
                    String.IsNullOrEmpty(DestinationContainerName) ? _defaultContainerName : DestinationContainerName);
            }
        }

        protected async Task WriteReport(JObject report, string name, Action<string> onWriting, Action<string> onWritten, Formatting formatting)
        {
            if (!String.IsNullOrEmpty(OutputDirectory))
            {
                await WriteToFile(report, name, onWriting, onWritten, formatting);
            }
            else
            {
                await DestinationContainer.CreateIfNotExistsAsync();
                await WriteToBlob(report, name, onWriting, onWritten, formatting);
            }
        }

        private async Task WriteToFile(JObject report, string name, Action<string> onWriting, Action<string> onWritten, Formatting formatting)
        {
            string fullPath = Path.Combine(OutputDirectory, name);
            string parentDir = Path.GetDirectoryName(fullPath);
            onWriting(fullPath);
            if (!WhatIf)
            {
                if (!Directory.Exists(parentDir))
                {
                    Directory.CreateDirectory(parentDir);
                }
                if (File.Exists(fullPath))
                {
                    File.Delete(fullPath);
                }
                using (var writer = new StreamWriter(File.OpenWrite(fullPath)))
                {
                    await writer.WriteAsync(report.ToString(formatting));
                }
            }
            onWritten(fullPath);
        }

        private async Task WriteToBlob(JObject report, string name, Action<string> onWriting, Action<string> onWritten, Formatting formatting)
        {
            var blob = DestinationContainer.GetBlockBlobReference(name);
            onWriting(blob.Uri.AbsoluteUri);
            if (!WhatIf)
            {
                blob.Properties.ContentType = MimeTypes.Json;
                await blob.UploadTextAsync(report.ToString(formatting));
            }
            onWritten(blob.Uri.AbsoluteUri);
        }
    }
}
