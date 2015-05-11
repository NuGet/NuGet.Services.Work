// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Configuration
{
    public class LicenseReportConfiguration
    {
        [Description("The url to the license report service")]
        public Uri Service { get; set; }
        [Description("The username for the license report service")]
        public string Username { get; set; }
        [Description("The password for the license report service")]
        public string Password { get; set; }
    }
}
