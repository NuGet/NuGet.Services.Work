// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace NuGet.Services.Work
{
    public class InvocationPayloadSerializerFacts
    {
        public static IEnumerable<object[]> SimpleSerializationData
        {
            get
            {
                yield return new object[] { 
                    new Dictionary<string, string>() { {"foo", "bar"} },
                    "{\"foo\":\"bar\"}"
                };
                yield return new object[] { 
                    new Dictionary<string, string>() { {"foo", null} },
                    "{\"foo\":null}"
                };
            }
        }

        [Theory]
        [PropertyData("SimpleSerializationData")]
        public void SimpleSerialization(Dictionary<string, string> payload, string expectedJson)
        {
            Assert.Equal(expectedJson, InvocationPayloadSerializer.Serialize(payload));
        }

        [Theory]
        [PropertyData("SimpleSerializationData")]
        public void SimpleDeserialization(Dictionary<string, string> expectedPayload, string json)
        {
            var deserialized = InvocationPayloadSerializer.Deserialize(json);
            Assert.True(expectedPayload.SequenceEqual(deserialized));
        }
    }
}
