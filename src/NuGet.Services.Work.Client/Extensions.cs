// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Linq;
using System.Text;

namespace System.Collections.Generic
{
    internal static class GenericCollectionExtensions
    {
        public static bool DictionariesEqual<K, V>(this Dictionary<K, V> self, Dictionary<K, V> other)
        {
            return DictionariesEqual(self, other, EqualityComparer<V>.Default);
        }

        public static bool DictionariesEqual<K, V>(this Dictionary<K, V> self, Dictionary<K, V> other, IEqualityComparer<V> valueComparer)
        {
            if (self == null)
            {
                return other == null;
            }
            else if (other == null)
            {
                return self == null;
            }
            else if (self.Count != other.Count)
            {
                return false;
            }
            else
            {
                return self.Keys.All(k =>
                {
                    V val;
                    if (!other.TryGetValue(k, out val))
                    {
                        return false; // Right does not have this key
                    }
                    return valueComparer.Equals(self[k], val); // Compare left and right values.
                });
            }
        }
    }
}
