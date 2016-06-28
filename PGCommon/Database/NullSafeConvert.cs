using System;

namespace PGCommon.Database
{
    public static class NullSafeConvert
    {
        public static T ConvertTo<T>(object value)
        {
            var t = typeof(T);
            var u = Nullable.GetUnderlyingType(t);

            if (u == null) return (T)Convert.ChangeType(value, t);
            if (value == null )
                return default(T);

            return (T)Convert.ChangeType(value, u);
        }
    }
}