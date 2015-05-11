// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Jobs.Models
{
    public interface IAssertionSet
    {
        string PackageId { get; }
        bool Exists { get; }
        HashSet<OwnerAssertion> Owners { get; set; }
    }

    public interface IPackageAssertionSet : IAssertionSet
    {
        string Version { get; }
    }
    /// <summary>
    /// NOTE THAT this assertion has the 'packageId' and 'Owners' information only
    /// This is the least common denominator of all assertions. Just the packageId and list of owners
    /// If the owners list is null or empty, only the packageId will be serialized
    /// This assertion will be directly used when there are only 'Remove Owner' assertions on a package
    /// Even there is 1 AddOwner assertion, its immediate derived class will be need to be used
    /// </summary>
    public class PackageOwnerAssertionSet : IAssertionSet
    {
        public PackageOwnerAssertionSet() { }
        internal PackageOwnerAssertionSet(string packageId)
        {
            PackageId = packageId;
        }

        public string PackageId { get; set; }

        public bool Exists
        {
            get
            {
                if (Owners == null || Owners.Count == 0)
                {
                    throw new InvalidOperationException("Owners cannot be null or empty");
                }
                return Owners.Where(o => o.Exists).FirstOrDefault() != null;
            }
        }

        public HashSet<OwnerAssertion> Owners { get; set; }

        public bool ShouldSerializeExists()
        {
            return Exists;
        }
    }

    /// <summary>
    /// NOTE THAT this assertion has the 'packageVersion' information in addition to the 'packageId', 'Owners' and 'Exists' information from its base classes
    /// This assertion will be directly used for a delete package assertion. If there are owner assertions, they will get added here as well. If not, they will be ignored
    /// This assertion does not contain the other information used during 'Add Package' or 'Edit Package' like 'Created Date' and so on
    /// </summary>
    public class PackageMinAssertionSet : IPackageAssertionSet
    {
        /// <summary>
        /// Adding a parameterless default constructor for supporting Dapper and have internal constructor for writing simple unit tests
        /// Could have added a constructor with a signature matching the sql query, but this is less code
        /// </summary>
        public PackageMinAssertionSet() { }
        internal PackageMinAssertionSet(string packageId, string version, bool exists)
        {
            PackageId = packageId;
            Version = version;
            Exists = exists;
        }

        [JsonProperty(Order = -2)]
        public string PackageId { get; set; }

        [JsonProperty(Order = -2)]
        public string Version { get; set; }

        [JsonProperty(Order = -2)]
        public bool Exists { get; set; }

        public HashSet<OwnerAssertion> Owners { get; set; }

        public bool ShouldSerializeOwners()
        {
            return Owners != null && Owners.Count > 0;
        }
    }

    /// <summary>
    /// This assertion is the full assertion containing all the possible fields and is used during 'Add Package' or 'Edit Package'
    /// As with all its base classes, the owners field will be ignored, if the Owners field is null or empty
    /// </summary>
    public class PackageAssertionSet : PackageMinAssertionSet
    {
        /// <summary>
        /// Adding a parameterless default constructor for supporting Dapper and have internal constructor for writing simple unit tests
        /// Could have added a constructor with a signature matching the sql query, but this is less code
        /// </summary>
        public PackageAssertionSet() { }
        internal PackageAssertionSet(string packageId, string version, bool exists) : base(packageId, version, exists) { }

        internal PackageAssertionSet(string packageId, string version, bool exists, object nupkg, bool listed, DateTime? created, DateTime? published)
            : base(packageId, version, exists)
        {
            Nupkg = nupkg;
            Listed = listed;
            Created = created;
            Published = published;
        }

        [JsonIgnore]
        public int Key { get; set; }

        [JsonIgnore]
        public int ProcessAttempts { get; set; }

        [JsonIgnore]
        public DateTime? FirstProcessingDateTime { get; set; }

        [JsonIgnore]
        public DateTime? LastProcessingDateTime { get; set; }

        [JsonIgnore]
        public DateTime? ProcessedDateTime { get; set; }

        public object Nupkg { get; set; }
        public bool Listed { get; set; }
        public DateTime? Created { get; set; }
        public DateTime? Published { get; set; }

        public DateTime? LastEdited { get; set; }
    }

    public class OwnerAssertion
    {
        /// <summary>
        /// Adding a parameterless default constructor for supporting Dapper and have internal constructor for writing simple unit tests
        /// Could have added a constructor with a signature matching the sql query, but this is less code
        /// </summary>
        public OwnerAssertion() { }
        internal OwnerAssertion(string username, bool exists)
        {
            Username = username;
            Exists = exists;
        }
        public string Username { get; set; }
        public bool Exists { get; set; }

        public override bool Equals(object obj)
        {
            var other = obj as OwnerAssertion;
            return Exists == other.Exists && String.Equals(Username, other.Username, StringComparison.OrdinalIgnoreCase);
        }

        public override int GetHashCode()
        {
            // Simplified: returning hash code of username only. Never will it be that the same user is both added and removed in an assertion set
            return Username.GetHashCode();
        }
    }

    public class PackageOwnerAssertion : OwnerAssertion
    {
        /// <summary>
        /// Adding a parameterless default constructor for supporting Dapper and have internal constructor for writing simple unit tests
        /// Could have added a constructor with a signature matching the sql query, but this is less code
        /// </summary>
        public PackageOwnerAssertion() { }

        internal PackageOwnerAssertion(string packageId, string version, string username, bool exists)
            : base(username, exists)
        {
            PackageId = packageId;
            Version = version;
        }
        [JsonIgnore]
        public int Key { get; set; }
        [JsonIgnore]
        public string PackageId { get; set; }
        [JsonIgnore]
        public string Version { get; set; }
    }
}
