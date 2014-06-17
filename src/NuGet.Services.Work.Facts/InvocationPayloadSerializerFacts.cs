using System.Collections.Generic;
using System.Linq;
using Xunit;

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
        [MemberData("SimpleSerializationData")]
        public void SimpleSerialization(Dictionary<string, string> payload, string expectedJson)
        {
            Assert.Equal(expectedJson, InvocationPayloadSerializer.Serialize(payload));
        }

        [Theory]
        [MemberData("SimpleSerializationData")]
        public void SimpleDeserialization(Dictionary<string, string> expectedPayload, string json)
        {
            var deserialized = InvocationPayloadSerializer.Deserialize(json);
            Assert.True(expectedPayload.SequenceEqual(deserialized));
        }
    }
}
