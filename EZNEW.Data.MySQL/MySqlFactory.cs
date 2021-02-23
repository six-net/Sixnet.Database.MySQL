using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using MySql.Data.MySqlClient;
using EZNEW.Data.CriteriaConverter;
using EZNEW.Develop.CQuery.CriteriaConverter;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Fault;
using EZNEW.Logging;
using EZNEW.Serialize;
using EZNEW.Develop.Command;
using EZNEW.Develop.CQuery;
using EZNEW.Develop.Entity;
using EZNEW.Develop.Command.Modify;
using EZNEW.Dapper;
using EZNEW.Develop.DataAccess;
using EZNEW.Diagnostics;

namespace EZNEW.Data.MySQL
{
    /// <summary>
    /// Database server factory
    /// </summary>
    internal static class MySqlFactory
    {
        /// <summary>
        /// Field format key
        /// </summary>
        internal static readonly string FieldFormatKey = ((int)DatabaseServerType.MySQL).ToString();

        /// <summary>
        /// Parameter prefix
        /// </summary>
        internal const string ParameterPrefix = "?";

        /// <summary>
        /// Key word prefix
        /// </summary>
        internal const string KeywordPrefix = "`";

        /// <summary>
        /// Key word suffix
        /// </summary>
        internal const string KeywordSuffix = "`";

        /// <summary>
        /// Paging table name
        /// </summary>
        internal const string PagingTableName = "EZNEW_TEMTABLE_PAGING";

        /// <summary>
        /// Calculate operators
        /// </summary>
        static readonly Dictionary<CalculateOperator, string> CalculateOperators = new Dictionary<CalculateOperator, string>(4)
        {
            [CalculateOperator.Add] = "+",
            [CalculateOperator.Subtract] = "-",
            [CalculateOperator.Multiply] = "*",
            [CalculateOperator.Divide] = "/",
        };

        /// <summary>
        /// Aggregate functions
        /// </summary>
        static readonly Dictionary<OperateType, string> AggregateFunctions = new Dictionary<OperateType, string>(5)
        {
            [OperateType.Max] = "MAX",
            [OperateType.Min] = "MIN",
            [OperateType.Sum] = "SUM",
            [OperateType.Avg] = "AVG",
            [OperateType.Count] = "COUNT",
        };

        /// <summary>
        /// Enable trace log
        /// </summary>
        static bool EnableTraceLog = false;

        /// <summary>
        /// Trace log split
        /// </summary>
        static readonly string TraceLogSplit = $"{new string('=', 10)} Database Command Translation Result {new string('=', 10)}";

        static MySqlFactory()
        {
            EnableTraceLog = SwitchManager.ShouldTraceFramework(sw =>
            {
                EnableTraceLog = SwitchManager.ShouldTraceFramework();
            });
        }

        #region Get database connection

        /// <summary>
        /// Get mysql database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        public static IDbConnection GetConnection(DatabaseServer server)
        {
            return DataManager.GetDatabaseConnection(server) ?? new MySqlConnection(server.ConnectionString);
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
            return DataManager.GetQueryTranslator(server.ServerType) ?? new MySqlQueryTranslator();
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
            return criteriaConverterParse(new CriteriaConverterParseOptions()
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
        static string Parse(CriteriaConverterParseOptions option)
        {
            if (string.IsNullOrWhiteSpace(option?.CriteriaConverter?.Name))
            {
                throw new EZNEWException("Criteria convert config name is null or empty");
            }
            string format = null;
            switch (option.CriteriaConverter.Name)
            {
                case CriteriaConverterNames.StringLength:
                    format = $"CHAR_LENGTH({option.ObjectName}.{WrapKeyword(option.FieldName)})";
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
            LogManager.LogInformation<MySqlProvider>(TraceLogSplit +
            $"{Environment.NewLine}{Environment.NewLine}{script}" +
            $"{Environment.NewLine}{Environment.NewLine}{parameters}" +
            $"{Environment.NewLine}{Environment.NewLine}");
        }

        #endregion

        #region Get command type

        /// <summary>
        /// Get command type
        /// </summary>
        /// <param name="command">Command</param>
        /// <returns>Return command type</returns>
        public static CommandType GetCommandType(RdbCommand command)
        {
            return command.CommandType == CommandTextType.Procedure ? CommandType.StoredProcedure : CommandType.Text;
        }

        #endregion

        #region Get calculate sign

        /// <summary>
        /// Get calculate sign
        /// </summary>
        /// <param name="calculate">Calculate operator</param>
        /// <returns></returns>
        public static string GetCalculateChar(CalculateOperator calculate)
        {
            CalculateOperators.TryGetValue(calculate, out var opearterChar);
            return opearterChar;
        }

        #endregion

        #region Get aggregate function name

        /// <summary>
        /// Get aggregate function name
        /// </summary>
        /// <param name="funcType">Function type</param>
        /// <returns></returns>
        public static string GetAggregateFunctionName(OperateType funcType)
        {
            AggregateFunctions.TryGetValue(funcType, out var funcName);
            return funcName;
        }

        #endregion

        #region Aggregate operate must need field

        /// <summary>
        /// Aggregate operate must need field
        /// </summary>
        /// <param name="operateType">Operate type</param>
        /// <returns></returns>
        public static bool AggregateOperateMustNeedField(OperateType operateType)
        {
            return operateType != OperateType.Count;
        }

        #endregion

        #region Format insert fields

        /// <summary>
        /// Format insert fields
        /// </summary>
        /// <param name="fields">Fields</param>
        /// <param name="parameters">Origin parameters</param>
        /// <param name="parameterSequence">Parameter sequence</param>
        /// <returns>first:fields,second:parameter fields,third:parameters</returns>
        public static Tuple<List<string>, List<string>, CommandParameters> FormatInsertFields(int fieldCount, IEnumerable<EntityField> fields, object parameters, int parameterSequence)
        {
            if (fields.IsNullOrEmpty())
            {
                return null;
            }
            List<string> formatFields = new List<string>(fieldCount);
            List<string> parameterFields = new List<string>(fieldCount);
            CommandParameters cmdParameters = ParseParameters(parameters);
            foreach (var field in fields)
            {
                formatFields.Add(WrapKeyword(field.FieldName));

                //parameter name
                parameterSequence++;
                string parameterName = field.PropertyName + parameterSequence;
                parameterFields.Add($"{ParameterPrefix}{parameterName}");

                //parameter value
                cmdParameters?.Rename(field.PropertyName, parameterName);
            }
            return new Tuple<List<string>, List<string>, CommandParameters>(formatFields, parameterFields, cmdParameters);
        }

        #endregion

        #region Format fields

        /// <summary>
        /// Format fields
        /// </summary>
        /// <param name="fields">Fields</param>
        /// <returns></returns>
        public static IEnumerable<string> FormatQueryFields(string databasePetName, IQuery query, Type entityType, bool forceMustFields, bool convertField)
        {
            if (query == null || entityType == null)
            {
                return Array.Empty<string>();
            }
            var queryFields = GetQueryFields(query, entityType, forceMustFields);
            return queryFields?.Select(field => FormatField(databasePetName, field, convertField)) ?? Array.Empty<string>();
        }

        /// <summary>
        /// Format query fields
        /// </summary>
        /// <param name="databasePetName">Database name</param>
        /// <param name="fields">Fields</param>
        /// <param name="convertField">Whether convert field</param>
        /// <returns></returns>
        public static IEnumerable<string> FormatQueryFields(string databasePetName, IEnumerable<EntityField> fields, bool convertField)
        {
            return fields?.Select(field => FormatField(databasePetName, field, convertField)) ?? Array.Empty<string>();
        }

        #endregion

        #region Format field

        /// <summary>
        /// Format field
        /// </summary>
        /// <param name="dataBaseObjectName">Database object name</param>
        /// <param name="field">field</param>
        /// <returns></returns>
        public static string FormatField(string dataBaseObjectName, EntityField field, bool convertField)
        {
            if (field == null)
            {
                return string.Empty;
            }
            var formatValue = $"{dataBaseObjectName}.{WrapKeyword(field.FieldName)}";
            if (!string.IsNullOrWhiteSpace(field.QueryFormat))
            {
                formatValue = string.Format(field.QueryFormat + " AS {1}", formatValue, WrapKeyword(field.PropertyName));
            }
            else if (field.FieldName != field.PropertyName && convertField)
            {
                formatValue = $"{formatValue} AS {WrapKeyword(field.PropertyName)}";
            }
            return formatValue;
        }

        #endregion

        #region Wrap keyword

        /// <summary>
        /// Wrap keyword by the KeywordPrefix and the KeywordSuffix
        /// </summary>
        /// <param name="originalValue">Original value</param>
        /// <returns></returns>
        internal static string WrapKeyword(string originalValue)
        {
            return $"{KeywordPrefix}{originalValue}{KeywordSuffix}";
        }

        #endregion

        #region Get fields

        /// <summary>
        /// Get query fields
        /// </summary>
        /// <param name="query">Query</param>
        /// <param name="entityType">Entity type</param>
        /// <param name="forceMustFields">Whether return must query fields</param>
        /// <returns></returns>
        public static IEnumerable<EntityField> GetQueryFields(IQuery query, Type entityType, bool forceMustFields)
        {
            return DataManager.GetQueryFields(DatabaseServerType.MySQL, entityType, query, forceMustFields);
        }

        /// <summary>
        /// Get fields
        /// </summary>
        /// <param name="entityType">Entity type</param>
        /// <param name="propertyNames">Property names</param>
        /// <returns></returns>
        public static IEnumerable<EntityField> GetFields(Type entityType, IEnumerable<string> propertyNames)
        {
            return DataManager.GetFields(DatabaseServerType.MySQL, entityType, propertyNames);
        }

        #endregion

        #region Get default field

        /// <summary>
        /// Get default field
        /// </summary>
        /// <param name="entityType">Entity type</param>
        /// <returns>Return default field name</returns>
        public static string GetDefaultFieldName(Type entityType)
        {
            if (entityType == null)
            {
                return string.Empty;
            }
            return DataManager.GetDefaultField(DatabaseServerType.MySQL, entityType)?.FieldName ?? string.Empty;
        }

        #endregion

        #region Format parameter name

        /// <summary>
        /// Format parameter name
        /// </summary>
        /// <param name="parameterName">Parameter name</param>
        /// <param name="parameterSequence">Parameter sequence</param>
        /// <returns></returns>
        public static string FormatParameterName(string parameterName, int parameterSequence)
        {
            return parameterName + parameterSequence;
        }

        #endregion

        #region Parse parameter

        /// <summary>
        /// Parse parameter
        /// </summary>
        /// <param name="originalParameters">Original parameter</param>
        /// <returns></returns>
        public static CommandParameters ParseParameters(object originalParameters)
        {
            if (originalParameters == null)
            {
                return null;
            }
            if (originalParameters is CommandParameters commandParameters)
            {
                return commandParameters;
            }
            commandParameters = new CommandParameters();
            if (originalParameters is IEnumerable<KeyValuePair<string, string>> stringParametersDict)
            {
                commandParameters.Add(stringParametersDict);
            }
            else if (originalParameters is IEnumerable<KeyValuePair<string, dynamic>> dynamicParametersDict)
            {
                commandParameters.Add(dynamicParametersDict);
            }
            else if (originalParameters is IEnumerable<KeyValuePair<string, object>> objectParametersDict)
            {
                commandParameters.Add(objectParametersDict);
            }
            else if (originalParameters is IEnumerable<KeyValuePair<string, IModifyValue>> modifyParametersDict)
            {
                commandParameters.Add(modifyParametersDict);
            }
            else
            {
                objectParametersDict = originalParameters.ObjectToDcitionary();
                commandParameters.Add(objectParametersDict);
            }
            return commandParameters;
        }

        #endregion

        #region Convert cmd parameters

        /// <summary>
        /// Convert cmd parameters
        /// </summary>
        /// <param name="commandParameters">Command parameters</param>
        /// <returns></returns>
        public static DynamicParameters ConvertCmdParameters(CommandParameters commandParameters)
        {
            if (commandParameters?.Parameters.IsNullOrEmpty() ?? true)
            {
                return null;
            }
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var item in commandParameters.Parameters)
            {
                var parameter = item.Value;
                dynamicParameters.Add(parameter.Name, parameter.Value
                                    , parameter.DbType, parameter.ParameterDirection
                                    , parameter.Size, parameter.Precision
                                    , parameter.Scale);
            }
            return dynamicParameters;
        }

        #endregion

        #region Get transaction isolation level

        /// <summary>
        /// Get transaction isolation level
        /// </summary>
        /// <param name="dataIsolationLevel">Data isolation level</param>
        /// <returns></returns>
        public static IsolationLevel? GetTransactionIsolationLevel(DataIsolationLevel? dataIsolationLevel)
        {
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.MySQL);
            }
            return DataManager.GetSystemIsolationLevel(dataIsolationLevel);
        }

        #endregion

        #region Get query transaction

        /// <summary>
        /// Get query transaction
        /// </summary>
        /// <param name="connection">Database connection</param>
        /// <param name="query">Query object</param>
        /// <returns></returns>
        public static IDbTransaction GetQueryTransaction(IDbConnection connection, IQuery query)
        {
            DataIsolationLevel? dataIsolationLevel = query?.IsolationLevel;
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.MySQL);
            }
            var systemIsolationLevel = GetTransactionIsolationLevel(dataIsolationLevel);
            if (systemIsolationLevel.HasValue)
            {
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }
                return connection.BeginTransaction(systemIsolationLevel.Value);
            }
            return null;
        }

        #endregion

        #region Get execute transaction

        /// <summary>
        /// Get execute transaction
        /// </summary>
        /// <param name="connection">Database connection</param>
        /// <param name="executeOption">Execute option</param>
        /// <returns></returns>
        public static IDbTransaction GetExecuteTransaction(IDbConnection connection, CommandExecuteOptions executeOption)
        {
            DataIsolationLevel? dataIsolationLevel = executeOption?.IsolationLevel;
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.MySQL);
            }
            var systemIsolationLevel = DataManager.GetSystemIsolationLevel(dataIsolationLevel);
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return systemIsolationLevel.HasValue ? connection.BeginTransaction(systemIsolationLevel.Value) : connection.BeginTransaction();
        }

        #endregion
    }
}
