// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace System.Data.SqlClient
{
    public class SqlConnectionStringBuilderExtensionsFacts
    {
        [Fact]
        public void TrimNetworkProtocolWithTcpColon()
        {
            // Arrange
            var cstr = new SqlConnectionStringBuilder("Data Source=tcp:blahblah.database.windows.net;Initial Catalog=NuGetDB");

            // Act
            cstr.TrimNetworkProtocol();

            // Assert
            Assert.True(cstr.DataSource.Equals("blahblah.database.windows.net"));
            Assert.True(cstr.InitialCatalog.Equals("NuGetDB"));
        }

        [Fact]
        public void TrimNetworkProtocolWithoutNetworkProtocol()
        {
            // Arrange
            var cstr = new SqlConnectionStringBuilder("Data Source=blahblah.database.windows.net;Initial Catalog=NuGetDB");

            // Act
            cstr.TrimNetworkProtocol();

            // Assert
            Assert.True(cstr.DataSource.Equals("blahblah.database.windows.net"));
            Assert.True(cstr.InitialCatalog.Equals("NuGetDB"));
        }
    }
}
