using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace PGCommon.Database
{
    public class SqlDatabase
    {
        private readonly string _connectionString;

        public SqlDatabase(string connectionString)
        {
            _connectionString = connectionString;
        }

        public SqlConnection CreateConnection()
        {
            return new SqlConnection(_connectionString);
        }

        private static IDbCommand CreateCommandByCommandType(CommandType commandType, string commandText)
        {
            DbCommand command = new SqlCommand();
            command.CommandType = commandType;
            command.CommandText = commandText;
            return command;
        }

        public IDbCommand GetSqlStringCommand(string query)
        {
            return CreateCommandByCommandType(CommandType.Text, query);
        }

        public IDbCommand GetStoredProcdureCommand(string query)
        {
            return CreateCommandByCommandType(CommandType.StoredProcedure, query);
        }

        public virtual int ExecuteNonQuery(IDbCommand command)
        {
            using (var connection = CreateAndOpenConnection())
            {
                command.Connection = connection;
                return command.ExecuteNonQuery();
            }
        }

        public SqlConnection CreateAndOpenConnection()
        {
            var connection = CreateConnection();
            connection.Open();
            return connection;
        }

        public IDataReader ExecuteReader(IDbCommand command)
        {
            using (var connection = CreateAndOpenConnection())
            {
                command.Connection = connection;
                return command.ExecuteReader();
            }
        }
    }
}