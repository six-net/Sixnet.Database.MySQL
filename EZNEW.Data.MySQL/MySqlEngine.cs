using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EZNEW.Dapper;
using EZNEW.Develop.Entity;
using EZNEW.Develop.CQuery;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Develop.Command;
using EZNEW.Develop.Command.Modify;
using EZNEW.Fault;
using EZNEW.Develop.DataAccess;
using EZNEW.Data.Configuration;
using EZNEW.Serialize;

namespace EZNEW.Data.MySQL
{
    /// <summary>
    /// Imeplements database engine for mysql
    /// </summary>
    public class MySqlEngine : IDatabaseEngine
    {
        static readonly string FieldFormatKey = ((int)DatabaseServerType.MySQL).ToString();
        const string ParameterPrefix = "?";
        const string PagingTableName = "EZNEW_TEMTABLE_PAGING";
        static readonly Dictionary<CalculateOperator, string> CalculateOperatorDictionary = new Dictionary<CalculateOperator, string>(4)
        {
            [CalculateOperator.Add] = "+",
            [CalculateOperator.Subtract] = "-",
            [CalculateOperator.Multiply] = "*",
            [CalculateOperator.Divide] = "/",
        };

        static readonly Dictionary<OperateType, string> AggregateFunctionDictionary = new Dictionary<OperateType, string>(5)
        {
            [OperateType.Max] = "MAX",
            [OperateType.Min] = "MIN",
            [OperateType.Sum] = "SUM",
            [OperateType.Avg] = "AVG",
            [OperateType.Count] = "COUNT",
        };

        #region Execute

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOption">Execute option</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return effected data numbers</returns>
        public int Execute(DatabaseServer server, CommandExecuteOption executeOption, IEnumerable<ICommand> commands)
        {
            return ExecuteAsync(server, executeOption, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOption">Execute option</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return effected data numbers</returns>
        public int Execute(DatabaseServer server, CommandExecuteOption executeOption, params ICommand[] commands)
        {
            return ExecuteAsync(server, executeOption, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOption">Execute option</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return effected data numbers</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecuteOption executeOption, IEnumerable<ICommand> commands)
        {
            #region group execute commands

            IQueryTranslator translator = MySqlFactory.GetQueryTranslator(server);
            List<DatabaseExecuteCommand> executeCommands = new List<DatabaseExecuteCommand>();
            var batchExecuteConfig = DataManager.GetBatchExecuteConfiguration(server.ServerType) ?? BatchExecuteConfiguration.Default;
            var groupStatementsCount = batchExecuteConfig.GroupStatementsCount;
            groupStatementsCount = groupStatementsCount < 0 ? 1 : groupStatementsCount;
            var groupParameterCount = batchExecuteConfig.GroupParametersCount;
            groupParameterCount = groupParameterCount < 0 ? 1 : groupParameterCount;
            StringBuilder commandTextBuilder = new StringBuilder();
            CommandParameters parameters = null;
            int statementsCount = 0;
            bool forceReturnValue = false;
            int cmdCount = 0;

            DatabaseExecuteCommand GetGroupExecuteCommand()
            {
                var executeCommand = new DatabaseExecuteCommand()
                {
                    CommandText = commandTextBuilder.ToString(),
                    CommandType = CommandType.Text,
                    ForceReturnValue = forceReturnValue,
                    Parameters = parameters
                };
                statementsCount = 0;
                translator.ParameterSequence = 0;
                commandTextBuilder.Clear();
                parameters = null;
                forceReturnValue = false;
                return executeCommand;
            }

            foreach (var cmd in commands)
            {
                DatabaseExecuteCommand executeCommand = GetExecuteDbCommand(translator, cmd as RdbCommand);
                if (executeCommand == null)
                {
                    continue;
                }

                //Trace log
                MySqlFactory.LogExecuteCommand(executeCommand);

                cmdCount++;
                if (executeCommand.PerformAlone)
                {
                    if (statementsCount > 0)
                    {
                        executeCommands.Add(GetGroupExecuteCommand());
                    }
                    executeCommands.Add(executeCommand);
                    continue;
                }
                commandTextBuilder.AppendLine(executeCommand.CommandText);
                parameters = parameters == null ? executeCommand.Parameters : parameters.Union(executeCommand.Parameters);
                forceReturnValue |= executeCommand.ForceReturnValue;
                statementsCount++;
                if (translator.ParameterSequence >= groupParameterCount || statementsCount >= groupStatementsCount)
                {
                    executeCommands.Add(GetGroupExecuteCommand());
                }
            }
            if (statementsCount > 0)
            {
                executeCommands.Add(GetGroupExecuteCommand());
            }

            #endregion

            return await ExecuteCommandAsync(server, executeOption, executeCommands, executeOption?.ExecuteByTransaction ?? cmdCount > 1).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOption">Execute option</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return effected data numbers</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecuteOption executeOption, params ICommand[] commands)
        {
            IEnumerable<ICommand> cmdCollection = commands;
            return await ExecuteAsync(server, executeOption, cmdCollection).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute commands
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeCommands">Execute commands</param>
        /// <param name="useTransaction">Use transaction</param>
        /// <returns>Return effected data numbers</returns>
        async Task<int> ExecuteCommandAsync(DatabaseServer server, CommandExecuteOption executeOption, IEnumerable<DatabaseExecuteCommand> executeCommands, bool useTransaction)
        {
            int resultValue = 0;
            bool success = true;
            using (var conn = MySqlFactory.GetConnection(server))
            {
                IDbTransaction transaction = null;
                if (useTransaction)
                {
                    transaction = GetExecuteTransaction(conn, executeOption);
                }
                try
                {
                    foreach (var cmd in executeCommands)
                    {
                        var cmdDefinition = new CommandDefinition(cmd.CommandText, ConvertCmdParameters(cmd.Parameters), transaction: transaction, commandType: cmd.CommandType, cancellationToken: executeOption?.CancellationToken ?? default);
                        var executeResultValue = await conn.ExecuteAsync(cmdDefinition).ConfigureAwait(false);
                        success = success && (cmd.ForceReturnValue ? executeResultValue > 0 : true);
                        resultValue += executeResultValue;
                        if (useTransaction && !success)
                        {
                            break;
                        }
                    }
                    if (!useTransaction)
                    {
                        return resultValue;
                    }
                    if (success)
                    {
                        transaction.Commit();
                    }
                    else
                    {
                        resultValue = 0;
                        transaction.Rollback();
                    }
                    return resultValue;
                }
                catch (Exception ex)
                {
                    resultValue = 0;
                    transaction?.Rollback();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Get execute database execute command
        /// </summary>
        /// <param name="command">Command</param>
        /// <returns>Return execute command</returns>
        DatabaseExecuteCommand GetExecuteDbCommand(IQueryTranslator queryTranslator, RdbCommand command)
        {
            DatabaseExecuteCommand GetTextCommand()
            {
                return new DatabaseExecuteCommand()
                {
                    CommandText = command.CommandText,
                    Parameters = ParseParameters(command.Parameters),
                    CommandType = GetCommandType(command),
                    ForceReturnValue = command.MustReturnValueOnSuccess,
                    HasPreScript = true
                };
            }
            if (command.ExecuteMode == CommandExecuteMode.CommandText)
            {
                return GetTextCommand();
            }
            DatabaseExecuteCommand executeCommand;
            switch (command.OperateType)
            {
                case OperateType.Insert:
                    executeCommand = GetInsertExecuteDbCommand(queryTranslator, command);
                    break;
                case OperateType.Update:
                    executeCommand = GetUpdateExecuteDbCommand(queryTranslator, command);
                    break;
                case OperateType.Delete:
                    executeCommand = GetDeleteExecuteDbCommand(queryTranslator, command);
                    break;
                default:
                    executeCommand = GetTextCommand();
                    break;
            }
            return executeCommand;
        }

        /// <summary>
        /// Get insert execute DbCommand
        /// </summary>
        /// <param name="translator">Translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return insert execute command</returns>
        DatabaseExecuteCommand GetInsertExecuteDbCommand(IQueryTranslator translator, RdbCommand command)
        {
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.MySQL, command.EntityType, command.ObjectName);
            var fields = DataManager.GetEditFields(DatabaseServerType.MySQL, command.EntityType);
            var insertFormatResult = FormatInsertFields(fields, command.Parameters, translator.ParameterSequence);
            if (insertFormatResult == null)
            {
                return null;
            }
            string cmdText = $"INSERT INTO `{objectName}` ({string.Join(",", insertFormatResult.Item1)}) VALUES ({string.Join(",", insertFormatResult.Item2)});";
            CommandParameters parameters = insertFormatResult.Item3;
            translator.ParameterSequence += fields.Count;
            return new DatabaseExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters
            };
        }

        /// <summary>
        /// Get update execute command
        /// </summary>
        /// <param name="translator">Translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return update execute command</returns>
        DatabaseExecuteCommand GetUpdateExecuteDbCommand(IQueryTranslator translator, RdbCommand command)
        {
            #region query translate

            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script 

            CommandParameters parameters = ParseParameters(command.Parameters) ?? new CommandParameters();
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.MySQL, command.EntityType, command.ObjectName);
            var fields = GetFields(command.EntityType, command.Fields);
            int parameterSequence = translator.ParameterSequence;
            List<string> updateSetArray = new List<string>();
            foreach (var field in fields)
            {
                var parameterValue = parameters.GetParameterValue(field.PropertyName);
                var parameterName = field.PropertyName;
                string newValueExpression = string.Empty;
                if (parameterValue != null)
                {
                    parameterSequence++;
                    parameterName = FormatParameterName(parameterName, parameterSequence);
                    parameters.Rename(field.PropertyName, parameterName);
                    if (parameterValue is IModifyValue)
                    {
                        var modifyValue = parameterValue as IModifyValue;
                        parameters.ModifyValue(parameterName, modifyValue.Value);
                        if (parameterValue is CalculateModifyValue)
                        {
                            var calculateModifyValue = parameterValue as CalculateModifyValue;
                            string calChar = GetCalculateChar(calculateModifyValue.Operator);
                            newValueExpression = $"{translator.ObjectPetName}.`{field.FieldName}`{calChar}{ParameterPrefix}{parameterName}";
                        }
                    }
                }
                if (string.IsNullOrWhiteSpace(newValueExpression))
                {
                    newValueExpression = $"{ParameterPrefix}{parameterName}";
                }
                updateSetArray.Add($"{translator.ObjectPetName}.`{field.FieldName}`={newValueExpression}");
            }
            string cmdText = $"{preScript}UPDATE `{objectName}` AS {translator.ObjectPetName} {joinScript} SET {string.Join(",", updateSetArray)} {conditionString};";
            translator.ParameterSequence = parameterSequence;

            #endregion

            #region parameter

            var queryParameters = ParseParameters(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        /// <summary>
        /// Get delete execute command
        /// </summary>
        /// <param name="translator">Translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return delete execute command</returns>
        DatabaseExecuteCommand GetDeleteExecuteDbCommand(IQueryTranslator translator, RdbCommand command)
        {
            #region query translate

            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.MySQL, command.EntityType, command.ObjectName);
            string cmdText = $"{preScript}DELETE {translator.ObjectPetName} FROM `{objectName}` AS {translator.ObjectPetName} {joinScript} {conditionString};";

            #endregion

            #region parameter

            CommandParameters parameters = ParseParameters(command.Parameters) ?? new CommandParameters();
            var queryParameters = ParseParameters(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        #endregion

        #region Query

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return datas</returns>
        public IEnumerable<T> Query<T>(DatabaseServer server, ICommand command)
        {
            return QueryAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return datas</returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            IQueryTranslator translator = MySqlFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    int size = command.Query.QuerySize;
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.MySQL, command.EntityType, command.ObjectName);
                    cmdText = $"{tranResult.PreScript}SELECT {string.Join(",", FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, out var defaultFieldName))} FROM `{objectName}` AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {(string.IsNullOrWhiteSpace(tranResult.OrderString) ? string.Empty : $"ORDER BY {tranResult.OrderString}")} {(size > 0 ? $"LIMIT 0,{size}" : string.Empty)}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = ConvertCmdParameters(ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            MySqlFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = MySqlFactory.GetConnection(server))
            {
                var tran = GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data paging
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data paging</returns>
        public IEnumerable<T> QueryPaging<T>(DatabaseServer server, ICommand command)
        {
            return QueryPagingAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query data paging
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data paging</returns>
        public async Task<IEnumerable<T>> QueryPagingAsync<T>(DatabaseServer server, ICommand command)
        {
            int beginIndex = 0;
            int pageSize = 1;
            if (command?.Query?.PagingInfo != null)
            {
                beginIndex = command.Query.PagingInfo.Page;
                pageSize = command.Query.PagingInfo.PageSize;
                beginIndex = (beginIndex - 1) * pageSize;
            }
            return await QueryOffsetAsync<T>(server, command, beginIndex, pageSize).ConfigureAwait(false);
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="size">Query size</param>
        /// <returns>Return datas</returns>
        public IEnumerable<T> QueryOffset<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            return QueryOffsetAsync<T>(server, command, offsetNum, size).Result;
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="size">Query size</param>
        /// <returns>Return datas</returns>
        public async Task<IEnumerable<T>> QueryOffsetAsync<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            if (command.Query == null)
            {
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            IQueryTranslator translator = MySqlFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);

            #endregion

            #region script

            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string limitString = $"LIMIT {offsetNum},{size}";
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.MySQL, command.EntityType, command.ObjectName);
                    List<string> formatQueryFields = FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, out var defaultFieldName);
                    var queryScript = $"SELECT {string.Join(", ", formatQueryFields)} FROM `{objectName}` AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")}";
                    cmdText = $"{(string.IsNullOrWhiteSpace(tranResult.PreScript) ? $"WITH {PagingTableName} AS ({queryScript})" : $"{tranResult.PreScript},{PagingTableName} AS ({queryScript})")}SELECT (SELECT COUNT({defaultFieldName}) FROM {PagingTableName}) AS QueryDataTotalCount,{translator.ObjectPetName}.* FROM {PagingTableName} AS {translator.ObjectPetName} ORDER BY {(string.IsNullOrWhiteSpace(tranResult.OrderString) ? $"{translator.ObjectPetName}.`{defaultFieldName}` DESC" : $"{tranResult.OrderString}")} {limitString}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = ConvertCmdParameters(ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            MySqlFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = MySqlFactory.GetConnection(server))
            {
                var tran = GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Determine whether data has existed
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether data has existed</returns>
        public bool Query(DatabaseServer server, ICommand command)
        {
            return QueryAsync(server, command).Result;
        }

        /// <summary>
        /// Determine whether data has existed
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether data has existed</returns>
        public async Task<bool> QueryAsync(DatabaseServer server, ICommand command)
        {
            var translator = MySqlFactory.GetQueryTranslator(server);

            #region query translate

            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString = $"WHERE {tranResult.ConditionString}";
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            var field = DataManager.GetDefaultField(DatabaseServerType.MySQL, command.EntityType);
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.MySQL, command.EntityType, command.ObjectName);
            string cmdText = $"{preScript}SELECT EXISTS(SELECT {translator.ObjectPetName}.`{field.FieldName}` FROM `{objectName}` AS {translator.ObjectPetName} {joinScript} {conditionString})";

            #endregion

            #region parameter

            var parameters = ConvertCmdParameters(ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            MySqlFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = MySqlFactory.GetConnection(server))
            {
                var tran = GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, cancellationToken: command.Query?.GetCancellationToken() ?? default);
                int value = await conn.ExecuteScalarAsync<int>(cmdDefinition).ConfigureAwait(false);
                return value > 0;
            }
        }

        /// <summary>
        /// Query single value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data</returns>
        public T AggregateValue<T>(DatabaseServer server, ICommand command)
        {
            return AggregateValueAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query single value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data</returns>
        public async Task<T> AggregateValueAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            IQueryTranslator translator = MySqlFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);

            #endregion

            #region script

            string cmdText;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string funcName = GetAggregateFunctionName(command.OperateType);
                    if (string.IsNullOrWhiteSpace(funcName))
                    {
                        return default;
                    }

                    #region field

                    EntityField field = null;
                    if (AggregateOperateMustNeedField(command.OperateType))
                    {
                        if (command.Query.QueryFields.IsNullOrEmpty())
                        {
                            throw new EZNEWException($"You must specify the field to perform for the {funcName} operation");
                        }
                        field = DataManager.GetField(DatabaseServerType.MySQL, command.EntityType, command.Query.QueryFields.First());
                    }
                    else
                    {
                        field = DataManager.GetDefaultField(DatabaseServerType.MySQL, command.EntityType);
                    }

                    #endregion

                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.MySQL, command.EntityType, command.ObjectName);
                    cmdText = $"{tranResult.PreScript}SELECT {funcName}({FormatField(translator.ObjectPetName, field)}) FROM `{objectName}` AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {(string.IsNullOrWhiteSpace(tranResult.OrderString) ? string.Empty : $"ORDER BY {tranResult.OrderString}")}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = ConvertCmdParameters(ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            MySqlFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = MySqlFactory.GetConnection(server))
            {
                var tran = GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.ExecuteScalarAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data set</returns>
        public async Task<DataSet> QueryMultipleAsync(DatabaseServer server, ICommand command)
        {
            //Trace log
            MySqlFactory.LogScript(command.CommandText, command.Parameters);
            using (var conn = MySqlFactory.GetConnection(server))
            {
                var tran = GetQueryTransaction(conn, command.Query);
                DynamicParameters parameters = ConvertCmdParameters(ParseParameters(command.Parameters));
                var cmdDefinition = new CommandDefinition(command.CommandText, parameters, transaction: tran, commandType: GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                using (var reader = await conn.ExecuteReaderAsync(cmdDefinition).ConfigureAwait(false))
                {
                    DataSet dataSet = new DataSet();
                    while (!reader.IsClosed && reader.Read())
                    {
                        DataTable dataTable = new DataTable();
                        dataTable.Load(reader);
                        dataSet.Tables.Add(dataTable);
                    }
                    return dataSet;
                }
            }
        }

        #endregion

        #region Util

        /// <summary>
        /// Get command type
        /// </summary>
        /// <param name="command">Command</param>
        /// <returns>Return command type</returns>
        CommandType GetCommandType(RdbCommand command)
        {
            return command.CommandType == CommandTextType.Procedure ? CommandType.StoredProcedure : CommandType.Text;
        }

        /// <summary>
        /// Get calculate sign
        /// </summary>
        /// <param name="calculate">Calculate operator</param>
        /// <returns></returns>
        string GetCalculateChar(CalculateOperator calculate)
        {
            CalculateOperatorDictionary.TryGetValue(calculate, out var opearterChar);
            return opearterChar;
        }

        /// <summary>
        /// Get aggregate function name
        /// </summary>
        /// <param name="funcType">Function type</param>
        /// <returns></returns>
        string GetAggregateFunctionName(OperateType funcType)
        {
            AggregateFunctionDictionary.TryGetValue(funcType, out var funcName);
            return funcName;
        }

        /// <summary>
        /// Aggregate operate must need field
        /// </summary>
        /// <param name="operateType">Operate type</param>
        /// <returns></returns>
        bool AggregateOperateMustNeedField(OperateType operateType)
        {
            return operateType != OperateType.Count;
        }

        /// <summary>
        /// Format insert fields
        /// </summary>
        /// <param name="fields">Fields</param>
        /// <param name="parameters">Origin parameters</param>
        /// <param name="parameterSequence">Parameter sequence</param>
        /// <returns>first:fields,second:parameter fields,third:parameters</returns>
        Tuple<List<string>, List<string>, CommandParameters> FormatInsertFields(List<EntityField> fields, object parameters, int parameterSequence)
        {
            if (fields.IsNullOrEmpty())
            {
                return null;
            }
            List<string> formatFields = new List<string>(fields.Count);
            List<string> parameterFields = new List<string>(fields.Count);
            CommandParameters cmdParameters = ParseParameters(parameters);
            foreach (var field in fields)
            {
                //fields
                var formatValue = field.GetEditFormat(FieldFormatKey);
                if (string.IsNullOrWhiteSpace(formatValue))
                {
                    formatValue = string.Format("`{0}`", field.FieldName);
                    field.SetEditFormat(FieldFormatKey, formatValue);
                }
                formatFields.Add(formatValue);

                //parameter name
                parameterSequence++;
                string parameterName = field.PropertyName + parameterSequence;
                parameterFields.Add($"{ParameterPrefix}{parameterName}");

                //parameter value
                cmdParameters?.Rename(field.PropertyName, parameterName);
            }
            return new Tuple<List<string>, List<string>, CommandParameters>(formatFields, parameterFields, cmdParameters);
        }

        /// <summary>
        /// Format fields
        /// </summary>
        /// <param name="fields">Fields</param>
        /// <returns></returns>
        List<string> FormatQueryFields(string dbObjectName, IQuery query, Type entityType, out string defaultFieldName)
        {
            defaultFieldName = string.Empty;
            if (query == null || entityType == null)
            {
                return new List<string>(0);
            }
            var queryFields = DataManager.GetQueryFields(DatabaseServerType.MySQL, entityType, query);
            if (queryFields.IsNullOrEmpty())
            {
                return new List<string>(0);
            }
            defaultFieldName = queryFields[0].FieldName;
            List<string> formatFields = new List<string>();
            foreach (var field in queryFields)
            {
                var formatValue = FormatField(dbObjectName, field);
                formatFields.Add(formatValue);
            }
            return formatFields;
        }

        /// <summary>
        /// Format field
        /// </summary>
        /// <param name="dataBaseObjectName">Database object name</param>
        /// <param name="field">field</param>
        /// <returns></returns>
        string FormatField(string dataBaseObjectName, EntityField field)
        {
            if (field == null)
            {
                return string.Empty;
            }
            var formatValue = field.GetQueryFormat(FieldFormatKey);
            if (string.IsNullOrWhiteSpace(formatValue))
            {
                string fieldName = $"{dataBaseObjectName}.{field.FieldName}";
                if (!string.IsNullOrWhiteSpace(field.QueryFormat))
                {
                    formatValue = string.Format(field.QueryFormat + " AS `{1}`", fieldName, field.PropertyName);
                }
                else if (field.FieldName != field.PropertyName)
                {
                    formatValue = $"{fieldName} AS `{field.PropertyName}`";
                }
                else
                {
                    formatValue = fieldName;
                }
                field.SetQueryFormat(FieldFormatKey, formatValue);
            }
            return formatValue;
        }

        /// <summary>
        /// Get fields
        /// </summary>
        /// <param name="entityType">Entity type</param>
        /// <param name="propertyNames">Property names</param>
        /// <returns></returns>
        List<EntityField> GetFields(Type entityType, IEnumerable<string> propertyNames)
        {
            return DataManager.GetFields(DatabaseServerType.MySQL, entityType, propertyNames);
        }

        /// <summary>
        /// Format parameter name
        /// </summary>
        /// <param name="parameterName">Parameter name</param>
        /// <param name="parameterSequence">Parameter sequence</param>
        /// <returns></returns>
        static string FormatParameterName(string parameterName, int parameterSequence)
        {
            return parameterName + parameterSequence;
        }

        /// <summary>
        /// Parse parameter
        /// </summary>
        /// <param name="originalParameters">Original parameter</param>
        /// <returns></returns>
        CommandParameters ParseParameters(object originalParameters)
        {
            if (originalParameters == null)
            {
                return null;
            }
            CommandParameters parameters = originalParameters as CommandParameters;
            if (parameters != null)
            {
                return parameters;
            }
            parameters = new CommandParameters();
            if (originalParameters is IEnumerable<KeyValuePair<string, string>>)
            {
                var stringParametersDict = originalParameters as IEnumerable<KeyValuePair<string, string>>;
                parameters.Add(stringParametersDict);
            }
            else if (originalParameters is IEnumerable<KeyValuePair<string, dynamic>>)
            {
                var dynamicParametersDict = originalParameters as IEnumerable<KeyValuePair<string, dynamic>>;
                parameters.Add(dynamicParametersDict);
            }
            else if (originalParameters is IEnumerable<KeyValuePair<string, object>>)
            {
                var objectParametersDict = originalParameters as IEnumerable<KeyValuePair<string, object>>;
                parameters.Add(objectParametersDict);
            }
            else if (originalParameters is IEnumerable<KeyValuePair<string, IModifyValue>>)
            {
                var modifyParametersDict = originalParameters as IEnumerable<KeyValuePair<string, IModifyValue>>;
                parameters.Add(modifyParametersDict);
            }
            else
            {
                var objectParametersDict = originalParameters.ObjectToDcitionary();
                parameters.Add(objectParametersDict);
            }
            return parameters;
        }

        /// <summary>
        /// Convert cmd parameters
        /// </summary>
        /// <param name="commandParameters">Command parameters</param>
        /// <returns></returns>
        DynamicParameters ConvertCmdParameters(CommandParameters commandParameters)
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

        /// <summary>
        /// Get transaction isolation level
        /// </summary>
        /// <param name="dataIsolationLevel">Data isolation level</param>
        /// <returns></returns>
        IsolationLevel? GetTransactionIsolationLevel(DataIsolationLevel? dataIsolationLevel)
        {
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.MySQL);
            }
            return DataManager.GetSystemIsolationLevel(dataIsolationLevel);
        }

        /// <summary>
        /// Get query transaction
        /// </summary>
        /// <param name="connection">Database connection</param>
        /// <param name="query">Query object</param>
        /// <returns></returns>
        IDbTransaction GetQueryTransaction(IDbConnection connection, IQuery query)
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

        /// <summary>
        /// Get execute transaction
        /// </summary>
        /// <param name="connection">Database connection</param>
        /// <param name="executeOption">Execute option</param>
        /// <returns></returns>
        IDbTransaction GetExecuteTransaction(IDbConnection connection, CommandExecuteOption executeOption)
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
