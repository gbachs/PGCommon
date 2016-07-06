using System;
using System.Data;

namespace PGCommon.Database
{
    public static class DbTransactionExtensions
    {
        public static IDbCommand CreateTextCommand(this IDbTransaction thisObj, string sql)
        {
            return CreateCommand(thisObj, sql, CommandType.Text);
        }

        public static IDbCommand CreateStoredProcedureCommand(this IDbTransaction thisObj, string storedProcedure)
        {
            return CreateCommand(thisObj, storedProcedure, CommandType.StoredProcedure);
        }

        private static IDbCommand CreateCommand(this IDbTransaction thisObj, string text, CommandType cmdType)
        {
            IDbCommand cmd = null;
            try
            {
                cmd = thisObj.Connection.CreateCommand();
                cmd.CommandText = text;
                cmd.CommandType = cmdType;
                cmd.Transaction = thisObj;
                return cmd;
            }
            catch (Exception)
            {
                cmd.TryDispose();
                throw;
            }
        }

        public static void ExecuteInReader(this IDbTransaction thisObj, string sql, Action<IDbCommand> createCommand, Action<IDataRecord> action)
        {
            using (var cmd = thisObj.CreateTextCommand(sql))
            {
                createCommand(cmd);

                using (var reader = cmd.ExecuteReader())
                    reader.AsEnumerable().ForEach(action);
            }
        }

        public static void ExecuteInReader(this IDbTransaction thisObj, string sql, Action<IDataRecord> action)
        {
            using (var cmd = thisObj.CreateTextCommand(sql))
            {
                using (var reader = cmd.ExecuteReader())
                    reader.AsEnumerable().ForEach(action);
            }
        }

        public static T ExecuteScalar<T>(this IDbTransaction thisObj, string sql, Action<IDbCommand> action)
        {
            using (var cmd = thisObj.CreateTextCommand(sql))
            {
                action(cmd);
                return cmd.ExecuteScalar<T>();
            }
        }

        public static T ExecuteScalar<T>(this IDbTransaction thisObj, Action<IDbCommand> action)
        {
            return thisObj.ExecuteScalar<T>(string.Empty, action);
        }

        public static int ExecuteNonQuery(this IDbTransaction thisObj, string sql, Action<IDbCommand> action)
        {
            using (var cmd = thisObj.CreateTextCommand(sql))
            {
                action(cmd);
                return cmd.ExecuteNonQuery();
            }
        }

        public static T ExecuteNonQuery<T>(this IDbTransaction thisObj, string sql, Action<IDbCommand> action, Func<IDataParameterCollection, T> output)
        {
            using (var cmd = thisObj.CreateTextCommand(sql))
            {
                action(cmd);
                cmd.ExecuteNonQuery();
                return output(cmd.Parameters);
            }
        }

        public static T ExecuteScalar<T>(this IDbTransaction thisObj, string sql)
        {
            return thisObj.ExecuteScalar<T>(sql, cmd => { });
        }

        public static int ExecuteNonQuery(this IDbTransaction thisObj, Action<IDbCommand> action)
        {
            return thisObj.ExecuteNonQuery(string.Empty, action);
        }

        public static int ExecuteNonQuery(this IDbTransaction thisObj, string sql)
        {
            using (var cmd = thisObj.CreateTextCommand(sql))
            {
                return cmd.ExecuteNonQuery();
            }
        }

        public static int ExecuteNonQuery(this IDbTransaction thisObj, string sql, int retryCount)
        {
            return ExecuteNonQuery(thisObj, sql, retryCount, 3000);
        }

        public static int ExecuteNonQuery(this IDbTransaction thisObj, string sql, int retryCount, int delayInMilliseconds)
        {
            using (var cmd = thisObj.CreateTextCommand(sql))
            {
                return cmd.ExecuteNonQuery(retryCount, delayInMilliseconds);
            }
        }

        public static IDataReader ExecuteReader(this IDbTransaction thisObj, string sql)
        {
            using (var cmd = thisObj.CreateTextCommand(sql))
            {
                return cmd.ExecuteReader();
            }
        }
    }
}