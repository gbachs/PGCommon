using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;

namespace PGCommon.Database
{
    public static class DbCommandExtensions
    {
        public static void AddInParam(this IDbCommand thisObj, string name, DbType type)
        {
            AddInParam(thisObj, name, type, null);
        }

        public static void AddInParam(this IDbCommand thisObj, string name, DbType type, object value)
        {
            IDataParameter param = thisObj.CreateDbParam(name, type, value);
            thisObj.Parameters.Add(param); //Do not add until after all values have been set, in case of exception
        }

        public static void AddInParam(this IDbCommand thisObj, string name, DbType type, object value, int size)
        {
            var param = thisObj.CreateDbParam(name, type, ParameterDirection.Input, value);
            param.Size = size;
            thisObj.Parameters.Add(param); //Do not add until after all values have been set, in case of exception
        }

        public static void AddOutParam(this IDbCommand thisObj, string name, DbType type)
        {
            thisObj.Parameters.Add(CreateDbParam(thisObj, name, type, ParameterDirection.Output, DBNull.Value));
        }

        public static void AddOutParam(this IDbCommand thisObj, String name, DbType type, int size)
        {
            var param = CreateDbParam(thisObj, name, type, ParameterDirection.Output, DBNull.Value);
            param.Size = size;
            thisObj.Parameters.Add(param);
        }

        public static IDbDataParameter CreateDbParam(this IDbCommand thisObj, string name, DbType type)
        {
            return CreateDbParam(thisObj, name, type, ParameterDirection.Input, null);
        }

        public static IDbDataParameter CreateDbParam(this IDbCommand thisObj, string name, DbType type, Object value)
        {
            return CreateDbParam(thisObj, name, type, ParameterDirection.Input, value);
        }

        public static IDbDataParameter CreateDbParam(this IDbCommand thisObj, string name, DbType type, ParameterDirection direction, Object value)
        {
            var param = thisObj.CreateParameter();
            param.ParameterName = name;
            param.DbType = type;
            param.Value = value ?? DBNull.Value;
            param.Direction = direction;
            return param;
        }

        public static T ExecuteScalar<T>(this IDbCommand thisObj)
        {
            var result = thisObj.ExecuteScalar();
            return NullSafeConvert.ConvertTo<T>(result);
        }

        public static T ExecuteScalarOrDefault<T>(this IDbCommand thisObj, T defaultValue)
        {
            var result = thisObj.ExecuteScalar();
            return (result == null) ? defaultValue : NullSafeConvert.ConvertTo<T>(result);
        }

        public static int ExecuteNonQuery(this IDbCommand thisObj, int retryCount)
        {
            if (thisObj.Transaction != null)
                return ExecuteNonQuery(thisObj, thisObj.Transaction.IsolationLevel, retryCount);
            return ExecuteNonQuery(thisObj, IsolationLevel.Unspecified, retryCount);
        }

        public static int ExecuteNonQuery(this IDbCommand thisObj, IsolationLevel isolationLevel, int retryCount)
        {
            return ExecuteNonQuery(thisObj, isolationLevel, retryCount, 3000);
        }

        public static int ExecuteNonQuery(this IDbCommand thisObj, int retryCount, int delayInMilliseconds)
        {
            if (thisObj.Transaction != null)
                return ExecuteNonQuery(thisObj, thisObj.Transaction.IsolationLevel, retryCount, delayInMilliseconds);
            return ExecuteNonQuery(thisObj, IsolationLevel.Unspecified, retryCount, delayInMilliseconds);
        }

        public static int ExecuteNonQuery(this IDbCommand thisObj, IsolationLevel isolationLevel, int retryCount, int delayInMilliseconds)
        {
            if (thisObj.Transaction != null)
                throw new InvalidOperationException("Command should not be associated with an existing transaction");

            const int deadlockErrorNumber = 1205;
            bool wasSuccessful = false;
            int retryCounter = 0, result = 0;
            while (!wasSuccessful && retryCounter < retryCount)
            {
                thisObj.Transaction = GetTransaction(thisObj, isolationLevel);

                try
                {
                    result = thisObj.ExecuteNonQuery();

                    if (thisObj.Transaction != null)
                        thisObj.Transaction.Commit();

                    wasSuccessful = true;
                }
                catch (SqlException ex)
                {
                    if (ex.Number != deadlockErrorNumber || retryCounter >= retryCount)
                    {
                        if (thisObj.Transaction != null)
                            thisObj.Transaction.Rollback();
                        throw;

                    }
                    thisObj.Cancel();

                    Thread.Sleep(delayInMilliseconds);  // set a delay
                    wasSuccessful = false;
                    retryCounter += 1;
                }
            }
            return result;
        }

        private static IDbTransaction GetTransaction(IDbCommand command, IsolationLevel isolationLevel)
        {
            return isolationLevel != IsolationLevel.Unspecified
                ? command.Connection.BeginTransaction(isolationLevel)
                : null;
        }
    }
}