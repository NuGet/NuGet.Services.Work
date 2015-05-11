// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Helpers
{
    public static class Utils
    {
        private static readonly Regex ServerNameMatcher = new Regex(@"(tcp:)?(?<servername>[A-Za-z0-9]*)(\.database\.windows\.net)?");
        public static string GetSqlServerName(string fullName)
        {
            var match = ServerNameMatcher.Match(fullName);
            if (match.Success)
            {
                return match.Groups["servername"].Value;
            }
            return fullName;
        }
    }
}
