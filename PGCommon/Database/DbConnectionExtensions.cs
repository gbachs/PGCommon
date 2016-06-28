using System;
using System.Data;

namespace PGCommon.Database
{
    public static class DbConnectionExtensions
    {
        public static IDbCommand CreateTextCommand(this IDbConnection thisObj, string sql)
        {
            return CreateCommand(thisObj, sql, CommandType.Text);
        }

        public static IDbCommand CreateStoredProcedureCommand(this IDbConnection thisObj, string storedProcedure)
        {
            return CreateCommand(thisObj, storedProcedure, CommandType.StoredProcedure);
        }

        internal static IDbCommand CreateCommand(this IDbConnection thisObj, string text, CommandType cmdType)
        {
            IDbCommand cmd = null;
            try
            {
                cmd = thisObj.CreateCommand();
                cmd.Connection = thisObj;
                cmd.CommandText = text;
                cmd.CommandType = cmdType;
                return cmd;
            }
            catch (Exception)
            {
                cmd.TryDispose();
                throw;
            }
        }

        public static T ExecuteScalar<T>(this IDbConnection thisObj, string sql, Action<IDbCommand> action)
        {
            using (var cmd = CreateTextCommand(thisObj, sql))
            {
                action(cmd);
                return cmd.ExecuteScalar<T>();
            }
        }

        public static T ExecuteScalar<T>(this IDbConnection thisObj, string sql)
        {
            return thisObj.ExecuteScalar<T>(sql, cmd => { });
        }

        public static T ExecuteScalar<T>(this IDbConnection thisObj, Action<IDbCommand> action)
        {
            return thisObj.ExecuteScalar<T>(string.Empty, action);
        }

        public static int ExecuteNonQuery(this IDbConnection thisObj, string sql, Action<IDbCommand> action)
        {
            using (var cmd = CreateTextCommand(thisObj, sql))
            {
                action(cmd);
                return cmd.ExecuteNonQuery();
            }
        }

        public static int ExecuteNonQuery(this IDbConnection thisObj, Action<IDbCommand> action)
        {
            return thisObj.ExecuteNonQuery(string.Empty, action);
        }

        public static int ExecuteNonQuery(this IDbConnection thisObj, string sql)
        {
            using (var cmd = CreateTextCommand(thisObj, sql))
            {
                return cmd.ExecuteNonQuery();
            }
        }

        public static int ExecuteNonQuery(this IDbConnection thisObj, string sql, int retryCount)
        {
            return ExecuteNonQuery(thisObj, sql, retryCount, 3000);
        }

        public static int ExecuteNonQuery(this IDbConnection thisObj, string sql, int retryCount, int delayInMilliseconds)
        {
            using (var cmd = CreateTextCommand(thisObj, sql))
            {
                return cmd.ExecuteNonQuery(retryCount, delayInMilliseconds);
            }
        }

        public static IDataReader ExecuteReader(this IDbConnection thisObj, string sql)
        {
            using (var cmd = CreateTextCommand(thisObj, sql))
            {
                return cmd.ExecuteReader();
            }
        }

        public static void WithinTransaction(this IDbConnection thisObj, Action<IDbTransaction> action)
        {
            using (var transaction = thisObj.BeginTransaction())
            {
                action(transaction);
                transaction.Commit();
            }
        }
    }
}
