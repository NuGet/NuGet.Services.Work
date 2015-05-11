// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using PowerArgs;

namespace NuGet.Services.Work.JobHost
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length > 0 && String.Equals(args[0], "dbg", StringComparison.OrdinalIgnoreCase))
            {
                args = args.Skip(1).ToArray();
                Debugger.Launch();
            }

            Arguments parsed;
            try
            {
                parsed = Args.Parse<Arguments>(args);
                AsyncMain(parsed).Wait();
            }
            catch (ArgException ex)
            {
                Console.WriteLine(ex.Message);
                WriteUsage();
            }
        }

        private static void WriteUsage()
        {
            ArgUsage.GenerateUsageFromTemplate<Arguments>().Write();

            Console.WriteLine();
            Console.WriteLine("Available jobs: ");
            var jobs = LocalWorkService.GetAllAvailableJobs();
            var maxName = jobs.Max(d => d.Name.Length);
            foreach (var job in jobs)
            {
                Console.WriteLine("* {0} {1}", job.Name.PadRight(maxName), job.Description);
            }
        }

        private static async Task AsyncMain(Arguments args)
        {
            if (!String.IsNullOrEmpty(args.EncodedPayload))
            {
                args.Payload = Encoding.UTF8.GetString(Convert.FromBase64String(args.EncodedPayload));
            }

            if (!String.IsNullOrEmpty(args.EncodedConfiguration))
            {
                args.Configuration = Encoding.UTF8.GetString(Convert.FromBase64String(args.EncodedConfiguration));
            }

            var configuration = InvocationPayloadSerializer.Deserialize(args.Configuration);

            var service = await LocalWorkService.Create(configuration);

            var tcs = new TaskCompletionSource<object>();

            string message = String.Format("Invoking job: {0}.", args.Job);
            Console.WriteLine(message);
            Console.WriteLine(new String('-', message.Length));

            Stopwatch sw = new Stopwatch();
            try
            {
                sw.Start();
                var observable = service.RunJob(args.Job, args.Payload);
                observable
                    .Subscribe(
                        evt => RenderEvent(evt),
                        ex => tcs.SetException(ex),
                        () => tcs.SetResult(null));
                await tcs.Task;
                sw.Stop();
            }
            catch (AggregateException aex)
            {
                Console.Error.WriteLine(aex.InnerException.ToString());
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
            }

            message = String.Format("Completed invocation of job {0} in {1}.", args.Job, sw.Elapsed);
            Console.WriteLine(new String('-', message.Length));
            Console.WriteLine(message);
        }

        private static readonly Dictionary<EventLevel, string> _levelMap = new Dictionary<EventLevel,string>() {
            { EventLevel.Critical, "fatal" },
            { EventLevel.Error, "error" },
            { EventLevel.Informational, "info" },
            { EventLevel.Verbose, "trace" },
            { EventLevel.Warning, "warn" }
        };
        private static readonly int _maxLen = _levelMap.Values.Max(s => s.Length);

        private static void RenderEvent(EventEntry evt)
        {
            string message = evt.FormattedMessage;
            string levelStr;
            if(!_levelMap.TryGetValue(evt.Schema.Level, out levelStr)) {
                levelStr = "????";
            }
            levelStr = levelStr.PadRight(_maxLen);
            Console.WriteLine("[{0}] {1}", levelStr, message);
        }
    }
}
