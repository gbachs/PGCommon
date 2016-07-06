using System;
using System.Data;

namespace PGCommon.Database
{
    public static class SqlDatabaseExtensions
    {
        public static void WithConnection(this SqlDatabase thisObj, Action<IDbConnection> action)
        {
            using (var connection = thisObj.CreateConnection())
            {
                connection.Open();
                action(connection);
            }
        }

        public static T WithConnection<T>(this SqlDatabase db, Func<IDbConnection, T> action)
        {
            using (var conn = db.CreateConnection())
            {
                conn.Open();
                return action(conn);
            }
        }

        public static void WithinTransaction(this SqlDatabase thisObj, Action<IDbTransaction> action)
        {
            thisObj.WithConnection(conn => conn.WithinTransaction(action));
        }

        public static T WithinTransaction<T>(this SqlDatabase db, Func<IDbTransaction, T> action)
        {
            return db.WithConnection(
                conn =>
                {
                    using (var transaction = conn.BeginTransaction())
                    {
                        var result = action(transaction);
                        transaction.Commit();
                        return result;
                    }
                });
        }

        public static void WithTextCommand(this SqlDatabase db, string sql, Action<IDbCommand> action)
        {
            db.WithConnection(conn =>
            {
                using (var cmd = conn.CreateTextCommand(sql))
                {
                    action(cmd);
                }
            });
        }

        public static T WithTextCommand<T>(this SqlDatabase db, string sql, Func<IDbCommand, T> action)
        {
            return db.WithConnection(conn =>
            {
                using (var cmd = conn.CreateTextCommand(sql))
                {
                    return action(cmd);
                }
            });
        }

        public static T ExecuteScalar<T>(this SqlDatabase db, string sql, Action<IDbCommand> action)
        {
            using (var conn = db.CreateConnection())
            {
                conn.Open();
                return conn.ExecuteScalar<T>(sql, action);
            }
        }

        public static void ExecuteInReader(this SqlDatabase db, string sql, Action<IDataRecord> action)
        {
            db.WithConnection(conn =>
            {
                using (var cmd = conn.CreateTextCommand(sql))
                {
                    using (var reader = cmd.ExecuteReader())
                        reader.AsEnumerable().ForEach(action);
                }
            });
        }

        public static void ExecuteInReader(this SqlDatabase db, string sql, Action<IDbCommand> createCommand, Action<IDataRecord> action)
        {
            db.WithConnection(conn =>
            {
                using (var cmd = conn.CreateTextCommand(sql))
                {
                    createCommand(cmd);

                    using (var reader = cmd.ExecuteReader())
                        reader.AsEnumerable().ForEach(action);
                }
            });
        }

        public static T ExecuteScalar<T>(this SqlDatabase db, string sql)
        {
            return db.ExecuteScalar<T>(sql, cmd => { });
        }

        public static T ExecuteScalar<T>(this SqlDatabase db, Action<IDbCommand> action)
        {
            return db.ExecuteScalar<T>(string.Empty, action);
        }

        public static int ExecuteNonQuery(this SqlDatabase db, string sql, Action<IDbCommand> action)
        {
            using (var cmd = db.GetSqlStringCommand(sql))
            {
                action(cmd);
                return db.ExecuteNonQuery(cmd);
            }
        }
        public static int ExecuteNonQuery(this SqlDatabase db, Action<IDbCommand> action)
        {
            return db.ExecuteNonQuery(string.Empty, action);
        }
    }
}