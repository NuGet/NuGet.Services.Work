// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Monitoring
{
    /// <summary>
    /// Writes lines to a specified EventSource method
    /// </summary>
    public class EventSourceWriter : TextWriter
    {
        private Action<string> _receiver;
        private StringBuilder _buffer = new StringBuilder();

        public override Encoding Encoding
        {
            get { return Encoding.Default; }
        }

        public EventSourceWriter(Action<string> receiver)
        {
            _receiver = receiver;
        }

        public override void Write(char value)
        {
            _buffer.Append(value);
            CheckFlushLine();
        }

        public override void Flush()
        {
            _receiver(_buffer.ToString().Trim());
            _buffer.Clear();
        }

        private void CheckFlushLine()
        {
            if (_buffer.ToString().EndsWith(NewLine))
            {
                // Flush the buffer
                Flush();
            }
        }
    }
}
