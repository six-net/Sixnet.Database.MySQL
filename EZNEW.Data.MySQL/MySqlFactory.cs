using System;
using System.Data;
using EZNEW.Serialize;
using MySql.Data.MySqlClient;
using EZNEW.Data.CriteriaConverter;
using EZNEW.Develop.CQuery.CriteriaConverter;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Fault;
using EZNEW.Logging;
using EZNEW.Develop.Command;

namespace EZNEW.Data.MySQL
{
    /// <summary>
    /// Database server factory
    /// </summary>
    internal static class MySqlFactory
    {
        /// <summary>
        /// Enable trace log
        /// </summary>
        static readonly bool EnableTraceLog = false;

        static readonly string TraceLogSplit = $"{new string('=', 10)} Database Command Translation Result {new string('=', 10)}";

        static MySqlFactory()
        {
            EnableTraceLog = TraceLogSwitchManager.ShouldTraceFramework();
        }

        #region Get database connection

        /// <summary>
        /// Get mysql database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        public static IDbConnection GetConnection(DatabaseServer server)
        {
            IDbConnection conn = DataManager.GetDatabaseConnection(server) ?? new MySqlConnection(server.ConnectionString);
            return conn;
        }

        #endregion

        #region Get query translator

        /// <summary>
        /// Get query translator
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns></returns>
        internal static IQueryTranslator GetQueryTranslator(DatabaseServer server)
        {
            var translator = DataManager.GetQueryTranslator(server.ServerType);
            if (translator == null)
            {
                translator = new MySqlQueryTranslator();
            }
            return translator;
        }

        #endregion

        #region Criteria converter

        /// <summary>
        /// Parse criteria converter
        /// </summary>
        /// <param name="converter">converter</param>
        /// <param name="objectName">object name</param>
        /// <param name="fieldName">field name</param>
        /// <returns></returns>
        internal static string ParseCriteriaConverter(ICriteriaConverter converter, string objectName, string fieldName)
        {
            var criteriaConverterParse = DataManager.GetCriteriaConverterParser(converter?.Name) ?? Parse;
            return criteriaConverterParse(new CriteriaConverterParseOption()
            {
                CriteriaConverter = converter,
                ServerType = DatabaseServerType.MySQL,
                ObjectName = objectName,
                FieldName = fieldName
            });
        }

        /// <summary>
        /// Parse
        /// </summary>
        /// <param name="option">parse option</param>
        /// <returns></returns>
        static string Parse(CriteriaConverterParseOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.CriteriaConverter?.Name))
            {
                throw new EZNEWException("Criteria convert config name is null or empty");
            }
            string format = null;
            switch (option.CriteriaConverter.Name)
            {
                case CriteriaConverterNames.StringLength:
                    format = $"CHAR_LENGTH({option.ObjectName}.`{option.FieldName}`)";
                    break;
            }
            if (string.IsNullOrWhiteSpace(format))
            {
                throw new EZNEWException($"Cann't resolve criteria convert:{option.CriteriaConverter.Name} for MySQL");
            }
            return format;
        }

        #endregion

        #region Command translation result log

        /// <summary>
        /// Log execute command
        /// </summary>
        /// <param name="executeCommand">Execte command</param>
        internal static void LogExecuteCommand(DatabaseExecuteCommand executeCommand)
        {
            if (EnableTraceLog)
            {
                LogScriptCore(executeCommand.CommandText, JsonSerializeHelper.ObjectToJson(executeCommand.Parameters));
            }
        }

        /// <summary>
        /// Log script
        /// </summary>
        /// <param name="script">Script</param>
        /// <param name="parameters">Parameters</param>
        internal static void LogScript(string script, object parameters)
        {
            if (EnableTraceLog)
            {
                LogScriptCore(script, JsonSerializeHelper.ObjectToJson(parameters));
            }
        }

        /// <summary>
        /// Log script
        /// </summary>
        /// <param name="script">Script</param>
        /// <param name="parameters">Parameters</param>
        static void LogScriptCore(string script, string parameters)
        {
            LogManager.LogInformation<MySqlEngine>(TraceLogSplit +
            $"{Environment.NewLine}{Environment.NewLine}{script}" +
            $"{Environment.NewLine}{Environment.NewLine}{parameters}" +
            $"{Environment.NewLine}{Environment.NewLine}");
        }

        #endregion
    }
}
