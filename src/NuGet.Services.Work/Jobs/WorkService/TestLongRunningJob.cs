// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Jobs
{
    /// <summary>
    /// Job used to confirm the worker is responding to requests
    /// </summary>
    [Description("A simple long-running job for testing")]
    public class TestLongRunningJob : JobHandler<TestLongRunningEventSource>
    {
        protected internal override async Task Execute()
        {
            // Extend the message lease to 10mins from now
            await Extend(TimeSpan.FromMinutes(10));

            // Sleep for 2 minutes, reporting that we're still here every 5 seconds
            for (int i = 0; i < 24; i++)
            {
                Log.StillRunning();
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
        }
    }

    [EventSource(Name="Outercurve-NuGet-Jobs-TestLongRunning")]
    public class TestLongRunningEventSource : EventSource
    {
        public static readonly TestLongRunningEventSource Log = new TestLongRunningEventSource();
        private TestLongRunningEventSource() { }

        [Event(
            eventId: 1,
            Message = "Still running")]
        public void StillRunning() { WriteEvent(1); }
    }
}
