using System;
using System.Collections.Generic;

namespace PGCommon.Database
{
    public static class EnumerableExtensions
    {
        public static void ForEach<TSource>(this IEnumerable<TSource> enumerable, Action<TSource> action)
        {
            foreach (var item in enumerable)
                action(item);
        }
    }
}