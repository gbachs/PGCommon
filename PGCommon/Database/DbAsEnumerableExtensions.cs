using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace PGCommon.Database
{
    public static class DbAsEnumerableExtensions
    {
        public static IEnumerable<IDataRecord> AsEnumerable(this IDataReader reader)
        {
            while (reader.Read())
                yield return reader;
        }

        public static IEnumerable<T> Select<T>(this IDataReader reader, Func<IDataRecord, T> selector)
        {
            return reader.AsEnumerable().Select(selector);
        }

        public static IEnumerable<IDataRecord> Select(this IDbCommand command)
        {
            return command.Select(r => r);
        }

        public static IEnumerable<T> Select<T>(this IDbCommand command, Func<IDataRecord, T> selector)
        {
            using (var reader = command.ExecuteReader())
            {
                foreach (var record in reader.Select(selector))
                    yield return record;
            }
        }

        public static IEnumerable<IDataRecord> Select(this IDbConnection conn, string sql)
        {
            return conn.Select(sql, cmd => { });
        }

        public static IEnumerable<IDataRecord> Select(this IDbConnection conn, Action<IDbCommand> setupCommand)
        {
            return conn.Select(null, setupCommand);
        }

        public static IEnumerable<IDataRecord> Select(this IDbConnection conn, string sql, Action<IDbCommand> setupCommand)
        {
            using (var cmd = conn.CreateTextCommand(sql))
            {
                setupCommand(cmd);
                foreach (var record in cmd.Select())
                    yield return record;
            }
        }

        public static IEnumerable<T> Select<T>(this IDbConnection conn, Action<IDbCommand> setupCommand, Func<IDataRecord, T> selector)
        {
            return conn.Select(setupCommand).Select(selector);
        }

        public static IEnumerable<T> Select<T>(this IDbConnection conn, string sql, Action<IDbCommand> setupCommand, Func<IDataRecord, T> selector)
        {
            return conn.Select(sql, setupCommand).Select(selector);
        }

        public static IEnumerable<T> Select<T>(this IDbConnection conn, Func<IDataRecord, T> selector)
        {
            return conn.Select(cmd => { }, selector);
        }

        public static IEnumerable<T> Select<T>(this IDbConnection conn, string sql, Func<IDataRecord, T> selector)
        {
            return conn.Select(sql, cmd => { }, selector);
        }

        public static IEnumerable<IDataRecord> Select(this IDbTransaction trans, string sql)
        {
            return trans.Select(sql, cmd => { });
        }

        public static IEnumerable<IDataRecord> Select(this IDbTransaction trans, Action<IDbCommand> setupCommand)
        {
            return trans.Select(null, setupCommand);
        }

        public static IEnumerable<IDataRecord> Select(this IDbTransaction trans, string sql, Action<IDbCommand> setupCommand)
        {
            using (var cmd = trans.CreateTextCommand(sql))
            {
                setupCommand(cmd);
                foreach (var record in cmd.Select())
                    yield return record;
            }
        }
        public static IEnumerable<T> Select<T>(this IDbTransaction trans, Action<IDbCommand> setupCommand, Func<IDataRecord, T> selector)
        {
            return trans.Select(setupCommand).Select(selector);
        }

        public static IEnumerable<T> Select<T>(this IDbTransaction trans, string sql, Action<IDbCommand> setupCommand, Func<IDataRecord, T> selector)
        {
            return trans.Select(sql, setupCommand).Select(selector);
        }

        public static IEnumerable<T> Select<T>(this IDbTransaction trans, Func<IDataRecord, T> selector)
        {
            return trans.Select(cmd => { }, selector);
        }

        public static IEnumerable<T> Select<T>(this IDbTransaction trans, string sql, Func<IDataRecord, T> selector)
        {
            return trans.Select(sql, cmd => { }, selector);
        }

        public static IEnumerable<IDataRecord> Select(this SqlDatabase db, string sql)
        {
            return db.Select(sql, cmd => { });
        }
        public static IEnumerable<IDataRecord> Select(this SqlDatabase db, Action<IDbCommand> setupCommand)
        {
            return db.Select(null, cmd => { });
        }

        public static IEnumerable<IDataRecord> Select(this SqlDatabase db, string sql, Action<IDbCommand> setupCommand)
        {
            using (var cmd = db.GetSqlStringCommand(sql))
            {
                setupCommand(cmd);
                using (var rdr = db.ExecuteReader(cmd))
                {
                    while (rdr.Read())
                        yield return rdr;
                }
            }
        }

        public static IEnumerable<T> Select<T>(this SqlDatabase db, Action<IDbCommand> setupCommand, Func<IDataRecord, T> selector)
        {
            return db.Select(setupCommand).Select(selector);
        }

        public static IEnumerable<T> Select<T>(this SqlDatabase db, string sql, Action<IDbCommand> setupCommand, Func<IDataRecord, T> selector)
        {
            return db.Select(sql, setupCommand).Select(selector);
        }

        public static IEnumerable<T> Select<T>(this SqlDatabase db, Func<IDataRecord, T> selector)
        {
            return db.Select(cmd => { }, selector);
        }

        public static IEnumerable<T> Select<T>(this SqlDatabase db, string sql, Func<IDataRecord, T> selector)
        {
            return db.Select(sql, cmd => { }, selector);
        }
    }
}