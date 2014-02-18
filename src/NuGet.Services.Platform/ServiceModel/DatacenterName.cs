using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace NuGet.Services.ServiceModel
{
    public struct DatacenterName : IEquatable<DatacenterName>
    {
        private static readonly Regex Parser = new Regex(@"^-(?<dc>[0-9]+)(?<rest>.+)?$", RegexOptions.IgnoreCase);

        public static readonly DatacenterName Empty = new DatacenterName();
        
        public EnvironmentName Environment { get; private set; }
        public int Id { get; private set; }

        public DatacenterName(EnvironmentName environment, int id)
            : this()
        {
            Guard.NonNegative(id, "id");

            Environment = environment;
            Id = id;
        }

        public override bool Equals(object obj)
        {
            return obj is DatacenterName && Equals((DatacenterName)obj);
        }

        public override int GetHashCode()
        {
            return HashCodeCombiner.Start()
                .Add(Environment)
                .Add(Id)
                .CombinedHash;
        }

        public bool Equals(DatacenterName other)
        {
            return Equals(Environment, other.Environment) &&
                Id == other.Id;
        }

        public override string ToString()
        {
            return Environment.ToString() + "-" + Id.ToString();
        }

        public static DatacenterName Parse(string input)
        {
            DatacenterName result;
            if (!TryParse(input, out result))
            {
                throw new FormatException(String.Format(
                    CultureInfo.CurrentCulture,
                    Strings.DatacenterName_InvalidName,
                    input));
            }
            return result;
        }

        public static bool TryParse(string input, out DatacenterName result)
        {
            string _;
            return TryParseCore(input, out result, out _);
        }

        internal static bool TryParseCore(string input, out DatacenterName result, out string remainder)
        {
            result = DatacenterName.Empty;
            remainder = null;

            // Parse the environment name portion
            EnvironmentName envName;
            string dcPart;
            if (!EnvironmentName.TryParseCore(input, out envName, out dcPart) || String.IsNullOrEmpty(dcPart))
            {
                return false;
            }

            var match = Parser.Match(dcPart);
            if (!match.Success)
            {
                return false;
            }
            else
            {
                result = new DatacenterName(
                    envName,
                    Int32.Parse(match.Groups["dc"].Value));
                if (match.Groups["rest"].Success)
                {
                    remainder = match.Groups["rest"].Value;
                }
                return true;
            }
        }
    }
}
