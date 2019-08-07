using EZNEW.Data.Config;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace EZNEW.Data.MySQL
{
    /// <summary>
    /// db server factory
    /// </summary>
    internal static class DbServerFactory
    {
        #region get db connection

        /// <summary>
        /// get mysql database connection
        /// </summary>
        /// <param name="server">database server</param>
        /// <returns>db connection</returns>
        public static IDbConnection GetConnection(ServerInfo server)
        {
            IDbConnection conn = DataManager.GetDBConnection?.Invoke(server) ?? new MySqlConnection(server.ConnectionString);
            return conn;
        }

        #endregion
    }
}
