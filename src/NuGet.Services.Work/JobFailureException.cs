// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work
{
    /// <summary>
    /// Thrown to indicate a general failure in a Work Server Job. NOT necessarily a base class used by all failure exceptions!
    /// </summary>
    [Serializable]
    public class JobFailureException : Exception
    {
        public JobFailureException() { }
        public JobFailureException(string message) : base(message) { }
        public JobFailureException(string message, Exception inner) : base(message, inner) { }
        protected JobFailureException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }
}
