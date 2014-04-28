using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Jobs
{
    [Description("Materializes a SQL Azure Database backup as a real online database.")]
    public class RestoreDatabaseBackupJob : JobHandler<RestoreDatabaseBackupEventSource>
    {
    }

    [EventSource(Name = "Outercurve-NuGet-Jobs-RestoreDatabaseBackup")]
    public class RestoreDatabaseBackupEventSource : EventSource
    {
        public static readonly RestoreDatabaseBackupEventSource Log = new RestoreDatabaseBackupEventSource();
        private RestoreDatabaseBackupEventSource() { }
    }
}
