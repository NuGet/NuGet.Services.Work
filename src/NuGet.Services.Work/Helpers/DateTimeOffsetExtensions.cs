// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace System
{
    public static class DateTimeOffsetExtensions
    {
        public static bool IsOlderThan(this DateTimeOffset self, TimeSpan age)
        {
            return (DateTimeOffset.UtcNow - self) > age;
        }

        public static bool IsYoungerThan(this DateTimeOffset self, TimeSpan age)
        {
            return (DateTimeOffset.UtcNow - self) < age;
        }
    }
}
