using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;
using System.IO;
using Dapper;
using MySql.Data.MySqlClient;
using EZNEW.Development.Entity;
using EZNEW.Development.Query;
using EZNEW.Development.Query.Translation;
using EZNEW.Development.Command;
using EZNEW.Exceptions;
using EZNEW.Data.Configuration;
using EZNEW.Application;
using EZNEW.Data.Modification;

namespace EZNEW.Data.MySQL
{
    /// <summary>
    /// Defines database provider implementation for mysql database(8.0+)
    /// </summary>
    public class MySqlProvider : IDatabaseProvider
    {
        const DatabaseServerType CurrentDatabaseServerType = MySqlManager.CurrentDatabaseServerType;

        #region Execute

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<ICommand> commands)
        {
            return ExecuteAsync(server, executionOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executionOptions, params ICommand[] commands)
        {
            return ExecuteAsync(server, executionOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executionOptions, params ICommand[] commands)
        {
            IEnumerable<ICommand> cmdCollection = commands;
            return await ExecuteAsync(server, executionOptions, cmdCollection).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<ICommand> commands)
        {
            #region group execution commands

            IQueryTranslator translator = MySqlManager.GetQueryTranslator(DataAccessContext.Create(server));
            List<DatabaseExecutionCommand> databaseExecutionCommands = new List<DatabaseExecutionCommand>();
            var batchExecutionConfig = DataManager.GetBatchExecutionConfiguration(server.ServerType) ?? BatchExecutionConfiguration.Default;
            var groupStatementsCount = batchExecutionConfig.GroupStatementsCount;
            groupStatementsCount = groupStatementsCount < 0 ? 1 : groupStatementsCount;
            var groupParameterCount = batchExecutionConfig.GroupParametersCount;
            groupParameterCount = groupParameterCount < 0 ? 1 : groupParameterCount;
            StringBuilder commandTextBuilder = new StringBuilder();
            CommandParameters parameters = null;
            int statementsCount = 0;
            bool forceReturnValue = false;
            int cmdCount = 0;

            DatabaseExecutionCommand GetGroupExecuteCommand()
            {
                var executionCommand = new DatabaseExecutionCommand()
                {
                    CommandText = commandTextBuilder.ToString(),
                    CommandType = CommandType.Text,
                    MustAffectedData = forceReturnValue,
                    Parameters = parameters
                };
                statementsCount = 0;
                translator.ParameterSequence = 0;
                commandTextBuilder.Clear();
                parameters = null;
                forceReturnValue = false;
                return executionCommand;
            }

            foreach (var cmd in commands)
            {
                DatabaseExecutionCommand executionCommand = GetDatabaseExecutionCommand(translator, cmd as DefaultCommand);
                if (executionCommand == null)
                {
                    continue;
                }

                //Trace log
                MySqlManager.LogExecutionCommand(executionCommand);

                cmdCount++;
                if (executionCommand.PerformAlone)
                {
                    if (statementsCount > 0)
                    {
                        databaseExecutionCommands.Add(GetGroupExecuteCommand());
                    }
                    databaseExecutionCommands.Add(executionCommand);
                    continue;
                }
                commandTextBuilder.AppendLine(executionCommand.CommandText);
                parameters = parameters == null ? executionCommand.Parameters : parameters.Union(executionCommand.Parameters);
                forceReturnValue |= executionCommand.MustAffectedData;
                statementsCount++;
                if (translator.ParameterSequence >= groupParameterCount || statementsCount >= groupStatementsCount)
                {
                    databaseExecutionCommands.Add(GetGroupExecuteCommand());
                }
            }
            if (statementsCount > 0)
            {
                databaseExecutionCommands.Add(GetGroupExecuteCommand());
            }

            #endregion

            return await ExecuteDatabaseCommandAsync(server, executionOptions, databaseExecutionCommands, executionOptions?.ExecutionByTransaction ?? cmdCount > 1).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute database command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="databaseExecutionCommands">Database execution commands</param>
        /// <param name="useTransaction">Whether use transaction</param>
        /// <returns>Return affected data number</returns>
        async Task<int> ExecuteDatabaseCommandAsync(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<DatabaseExecutionCommand> databaseExecutionCommands, bool useTransaction)
        {
            int totalAffectedNumber = 0;
            bool success = true;
            using (var conn = MySqlManager.GetConnection(server))
            {
                IDbTransaction transaction = null;
                if (useTransaction)
                {
                    transaction = MySqlManager.GetExecutionTransaction(conn, executionOptions);
                }
                try
                {
                    foreach (var executionCommand in databaseExecutionCommands)
                    {
                        var cmdDefinition = new CommandDefinition(executionCommand.CommandText, MySqlManager.ConvertCmdParameters(executionCommand.Parameters), transaction: transaction, commandType: executionCommand.CommandType, cancellationToken: executionOptions?.CancellationToken ?? default);
                        var affectedNumber = await conn.ExecuteAsync(cmdDefinition).ConfigureAwait(false);
                        success = success && (!executionCommand.MustAffectedData || affectedNumber > 0);
                        totalAffectedNumber += affectedNumber;
                        if (useTransaction && !success)
                        {
                            break;
                        }
                    }
                    if (!useTransaction)
                    {
                        return totalAffectedNumber;
                    }
                    if (success)
                    {
                        transaction.Commit();
                    }
                    else
                    {
                        totalAffectedNumber = 0;
                        transaction.Rollback();
                    }
                    return totalAffectedNumber;
                }
                catch (Exception ex)
                {
                    totalAffectedNumber = 0;
                    transaction?.Rollback();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Get database execution command
        /// </summary>
        /// <param name="queryTranslator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database execution command</returns>
        DatabaseExecutionCommand GetDatabaseExecutionCommand(IQueryTranslator queryTranslator, DefaultCommand command)
        {
            DatabaseExecutionCommand GetTextCommand()
            {
                return new DatabaseExecutionCommand()
                {
                    CommandText = command.Text,
                    Parameters = MySqlManager.ConvertParameter(command.Parameters),
                    CommandType = MySqlManager.GetCommandType(command),
                    MustAffectedData = command.MustAffectedData,
                    HasPreScript = true
                };
            }
            if (command.ExecutionMode == CommandExecutionMode.CommandText)
            {
                return GetTextCommand();
            }
            DatabaseExecutionCommand databaseExecutionCommand;
            switch (command.OperationType)
            {
                case CommandOperationType.Insert:
                    databaseExecutionCommand = GetDatabaseInsertionCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Update:
                    databaseExecutionCommand = GetDatabaseUpdateCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Delete:
                    databaseExecutionCommand = GetDatabaseDeletionCommand(queryTranslator, command);
                    break;
                default:
                    databaseExecutionCommand = GetTextCommand();
                    break;
            }
            return databaseExecutionCommand;
        }

        /// <summary>
        /// Get database insertion execution command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database insertion command</returns>
        DatabaseExecutionCommand GetDatabaseInsertionCommand(IQueryTranslator translator, DefaultCommand command)
        {
            translator.DataAccessContext.SetCommand(command);
            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            var fields = DataManager.GetEditFields(CurrentDatabaseServerType, command.EntityType);
            var fieldCount = fields.GetCount();
            var insertFormatResult = MySqlManager.FormatInsertionFields(command.EntityType, fieldCount, fields, command.Parameters, translator.ParameterSequence);
            if (insertFormatResult == null)
            {
                return null;
            }
            string cmdText = $"INSERT INTO {MySqlManager.WrapKeyword(objectName)} ({string.Join(",", insertFormatResult.Item1)}) VALUES ({string.Join(",", insertFormatResult.Item2)});";
            CommandParameters parameters = insertFormatResult.Item3;
            translator.ParameterSequence += fieldCount;
            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = MySqlManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters
            };
        }

        /// <summary>
        /// Get database update command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database update command</returns>
        DatabaseExecutionCommand GetDatabaseUpdateCommand(IQueryTranslator translator, DefaultCommand command)
        {
            if (command?.Fields.IsNullOrEmpty() ?? true)
            {
                throw new EZNEWException($"No fields are set to update");
            }

            #region query translation

            translator.DataAccessContext.SetCommand(command);
            var queryTranslationResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString))
            {
                conditionString += "WHERE " + queryTranslationResult.ConditionString;
            }
            string preScript = queryTranslationResult.PreScript;
            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;

            #endregion

            #region script 

            CommandParameters parameters = MySqlManager.ConvertParameter(command.Parameters) ?? new CommandParameters();
            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            var fields = MySqlManager.GetFields(command.EntityType, command.Fields);
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
                    parameterName = MySqlManager.FormatParameterName(parameterName, parameterSequence);
                    parameters.Rename(field.PropertyName, parameterName);
                    if (parameterValue is IModificationValue)
                    {
                        var modificationValue = parameterValue as IModificationValue;
                        parameters.ModifyValue(parameterName, modificationValue.Value);
                        if (parameterValue is CalculationModificationValue)
                        {
                            var calculationModificationValue = parameterValue as CalculationModificationValue;
                            string systemCalculationOperator = MySqlManager.GetSystemCalculationOperator(calculationModificationValue.Operator);
                            newValueExpression = $"{translator.ObjectPetName}.{MySqlManager.WrapKeyword(field.FieldName)}{systemCalculationOperator}{MySqlManager.ParameterPrefix}{parameterName}";
                        }
                    }
                }
                if (string.IsNullOrWhiteSpace(newValueExpression))
                {
                    newValueExpression = $"{MySqlManager.ParameterPrefix}{parameterName}";
                }
                updateSetArray.Add($"{translator.ObjectPetName}.{MySqlManager.WrapKeyword(field.FieldName)}={newValueExpression}");
            }
            string cmdText = $"{preScript}UPDATE {MySqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} SET {string.Join(",", updateSetArray)} {conditionString};";
            translator.ParameterSequence = parameterSequence;

            #endregion

            #region parameter

            var queryParameters = MySqlManager.ConvertParameter(queryTranslationResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = MySqlManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        /// <summary>
        /// Get database deletion command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database deletion command</returns>
        DatabaseExecutionCommand GetDatabaseDeletionCommand(IQueryTranslator translator, DefaultCommand command)
        {
            translator.DataAccessContext.SetCommand(command);

            #region query translation

            var queryTranslationResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString))
            {
                conditionString += "WHERE " + queryTranslationResult.ConditionString;
            }
            string preScript = queryTranslationResult.PreScript;
            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            string cmdText = $"{preScript}DELETE {translator.ObjectPetName} FROM {MySqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {conditionString};";

            #endregion

            #region parameter

            CommandParameters parameters = MySqlManager.ConvertParameter(command.Parameters) ?? new CommandParameters();
            var queryParameters = MySqlManager.ConvertParameter(queryTranslationResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = MySqlManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
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
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            IQueryTranslator translator = MySqlManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var queryTranslationResult = translator.Translate(command.Query);
            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = queryTranslationResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    int size = command.Query.QuerySize;
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    string sortString = string.IsNullOrWhiteSpace(queryTranslationResult.SortString) ? string.Empty : $"ORDER BY {queryTranslationResult.SortString}";
                    var queryFields = MySqlManager.GetQueryFields(command.Query, command.EntityType, true);
                    string outputFormatedField = string.Join(",", MySqlManager.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    if (string.IsNullOrWhiteSpace(queryTranslationResult.CombineScript))
                    {
                        cmdText = $"{queryTranslationResult.PreScript}SELECT {outputFormatedField} FROM {MySqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString) ? string.Empty : $"WHERE {queryTranslationResult.ConditionString}")} {sortString} {(size > 0 ? $"LIMIT 0,{size}" : string.Empty)}";
                    }
                    else
                    {
                        string innerFormatedField = string.Join(",", MySqlManager.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                        cmdText = $"{queryTranslationResult.PreScript}SELECT {outputFormatedField} FROM (SELECT {innerFormatedField} FROM {MySqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString) ? string.Empty : $"WHERE {queryTranslationResult.ConditionString}")} {queryTranslationResult.CombineScript}) AS {translator.ObjectPetName} {sortString} {(size > 0 ? $"LIMIT 0,{size}" : string.Empty)}";
                    }
                    break;
            }

            #endregion

            #region parameter

            var parameters = MySqlManager.ConvertCmdParameters(MySqlManager.ConvertParameter(queryTranslationResult.Parameters));

            #endregion

            //Trace log
            MySqlManager.LogScript(cmdText, queryTranslationResult.Parameters);

            using (var conn = MySqlManager.GetConnection(server))
            {
                var tran = MySqlManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: MySqlManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query paging data
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return paging data</returns>
        public IEnumerable<T> QueryPaging<T>(DatabaseServer server, ICommand command)
        {
            return QueryPagingAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query paging data
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return paging data</returns>
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
        /// <returns>Return the datas</returns>
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
        /// <returns>Return the datas</returns>
        public async Task<IEnumerable<T>> QueryOffsetAsync<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            IQueryTranslator translator = MySqlManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var queryTranslationResult = translator.Translate(command.Query);

            #endregion

            #region script

            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;
            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = queryTranslationResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string limitString = $"LIMIT {offsetNum},{size}";
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    string defaultFieldName = MySqlManager.GetDefaultFieldName(command.EntityType);
                    var queryFields = MySqlManager.GetQueryFields(command.Query, command.EntityType, true);
                    string innerFormatedField = string.Join(",", MySqlManager.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                    string outputFormatedField = string.Join(",", MySqlManager.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    string queryScript = $"SELECT {innerFormatedField} FROM {MySqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString) ? string.Empty : $"WHERE {queryTranslationResult.ConditionString}")} {queryTranslationResult.CombineScript}";
                    cmdText = $"{(string.IsNullOrWhiteSpace(queryTranslationResult.PreScript) ? $"WITH {MySqlManager.PagingTableName} AS ({queryScript})" : $"{queryTranslationResult.PreScript},{MySqlManager.PagingTableName} AS ({queryScript})")}SELECT (SELECT COUNT({MySqlManager.WrapKeyword(defaultFieldName)}) FROM {MySqlManager.PagingTableName}) AS {DataManager.PagingTotalCountFieldName},{outputFormatedField} FROM {MySqlManager.PagingTableName} AS {translator.ObjectPetName} ORDER BY {(string.IsNullOrWhiteSpace(queryTranslationResult.SortString) ? $"{translator.ObjectPetName}.{MySqlManager.WrapKeyword(defaultFieldName)} DESC" : $"{queryTranslationResult.SortString}")} {limitString}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = MySqlManager.ConvertCmdParameters(MySqlManager.ConvertParameter(queryTranslationResult.Parameters));

            #endregion

            //Trace log
            MySqlManager.LogScript(cmdText, queryTranslationResult.Parameters);

            using (var conn = MySqlManager.GetConnection(server))
            {
                var tran = MySqlManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: MySqlManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Indecats whether exists data
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether exists data</returns>
        public bool Exists(DatabaseServer server, ICommand command)
        {
            return ExistsAsync(server, command).Result;
        }

        /// <summary>
        /// Indecats whether exists data
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether exists data</returns>
        public async Task<bool> ExistsAsync(DatabaseServer server, ICommand command)
        {

            #region query translation

            var translator = MySqlManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            command.Query.ClearQueryFields();
            var queryFields = EntityManager.GetPrimaryKeys(command.EntityType).ToArray();
            if (queryFields.IsNullOrEmpty())
            {
                queryFields = EntityManager.GetQueryFields(command.EntityType).ToArray();
            }
            command.Query.AddQueryFields(queryFields);
            var queryTranslationResult = translator.Translate(command.Query);
            string conditionString = string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString) ? string.Empty : $"WHERE {queryTranslationResult.ConditionString}";
            string preScript = queryTranslationResult.PreScript;
            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            string cmdText = $"{preScript}SELECT EXISTS(SELECT {string.Join(",", MySqlManager.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {MySqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {conditionString} {queryTranslationResult.CombineScript})";

            #endregion

            #region parameter

            var parameters = MySqlManager.ConvertCmdParameters(MySqlManager.ConvertParameter(queryTranslationResult.Parameters));

            #endregion

            //Trace log
            MySqlManager.LogScript(cmdText, queryTranslationResult.Parameters);

            using (var conn = MySqlManager.GetConnection(server))
            {
                var tran = MySqlManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, cancellationToken: command.Query?.GetCancellationToken() ?? default);
                int value = await conn.ExecuteScalarAsync<int>(cmdDefinition).ConfigureAwait(false);
                return value > 0;
            }
        }

        /// <summary>
        /// Query aggregation value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return aggregation value</returns>
        public T AggregateValue<T>(DatabaseServer server, ICommand command)
        {
            return AggregateValueAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query aggregation value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return aggregation value</returns>
        public async Task<T> AggregateValueAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            bool queryObject = command.Query.ExecutionMode == QueryExecutionMode.QueryObject;
            string funcName = MySqlManager.GetAggregationFunctionName(command.OperationType);
            EntityField defaultField = null;
            if (queryObject)
            {
                if (string.IsNullOrWhiteSpace(funcName))
                {
                    throw new NotSupportedException($"Not support {command.OperationType}");
                }
                if (MySqlManager.CheckAggregationOperationMustNeedField(command.OperationType))
                {
                    if (command.Query.QueryFields.IsNullOrEmpty())
                    {
                        throw new EZNEWException($"Must specify the field to perform for the {funcName} operation");
                    }
                    defaultField = DataManager.GetField(CurrentDatabaseServerType, command.EntityType, command.Query.QueryFields.First());
                }
                else
                {
                    defaultField = DataManager.GetDefaultField(CurrentDatabaseServerType, command.EntityType);
                }
                //combine fields
                if (!command.Query.Combines.IsNullOrEmpty())
                {
                    var combineKeys = EntityManager.GetPrimaryKeys(command.EntityType).Union(new string[1] { defaultField.PropertyName }).ToArray();
                    command.Query.ClearQueryFields();
                    foreach (var combineEntry in command.Query.Combines)
                    {
                        combineEntry.Query.ClearQueryFields();
                        if (combineKeys.IsNullOrEmpty())
                        {
                            combineEntry.Query.ClearNotQueryFields();
                            command.Query.ClearNotQueryFields();
                        }
                        else
                        {
                            command.Query.AddQueryFields(combineKeys);
                            if (combineEntry.Type == CombineType.Union || combineEntry.Type == CombineType.UnionAll)
                            {
                                combineEntry.Query.AddQueryFields(combineKeys);
                            }
                        }
                    }
                }
            }
            IQueryTranslator translator = MySqlManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var queryTranslationResult = translator.Translate(command.Query);

            #endregion

            #region script

            string cmdText;
            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = queryTranslationResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    cmdText = string.IsNullOrWhiteSpace(queryTranslationResult.CombineScript)
                        ? $"{queryTranslationResult.PreScript}SELECT {funcName}({MySqlManager.FormatField(translator.ObjectPetName, defaultField, false)}) FROM {MySqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString) ? string.Empty : $"WHERE {queryTranslationResult.ConditionString}")}"
                        : $"{queryTranslationResult.PreScript}SELECT {funcName}({MySqlManager.FormatField(translator.ObjectPetName, defaultField, false)}) FROM (SELECT {string.Join(",", MySqlManager.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {MySqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString) ? string.Empty : $"WHERE {queryTranslationResult.ConditionString}")} {queryTranslationResult.CombineScript}) AS {translator.ObjectPetName}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = MySqlManager.ConvertCmdParameters(MySqlManager.ConvertParameter(queryTranslationResult.Parameters));

            #endregion

            //Trace log
            MySqlManager.LogScript(cmdText, queryTranslationResult.Parameters);

            using (var conn = MySqlManager.GetConnection(server))
            {
                var tran = MySqlManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: MySqlManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.ExecuteScalarAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data set
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return the datadset</returns>
        public async Task<DataSet> QueryMultipleAsync(DatabaseServer server, ICommand command)
        {
            //Trace log
            MySqlManager.LogScript(command.Text, command.Parameters);
            using (var conn = MySqlManager.GetConnection(server))
            {
                var tran = MySqlManager.GetQueryTransaction(conn, command.Query);
                DynamicParameters parameters = MySqlManager.ConvertCmdParameters(MySqlManager.ConvertParameter(command.Parameters));
                var cmdDefinition = new CommandDefinition(command.Text, parameters, transaction: tran, commandType: MySqlManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
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

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertionOptions">Bulk insertion options</param>
        public void BulkInsert(DatabaseServer server, DataTable dataTable, IBulkInsertionOptions bulkInsertionOptions = null)
        {
            BulkInsertAsync(server, dataTable).Wait();
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertionOptions">Bulk insertion options</param>
        public async Task BulkInsertAsync(DatabaseServer server, DataTable dataTable, IBulkInsertionOptions bulkInsertionOptions = null)
        {
            if (server == null)
            {
                throw new ArgumentNullException(nameof(server));
            }
            if (dataTable == null)
            {
                throw new ArgumentNullException(nameof(dataTable));
            }
            var dataFilePath = dataTable.WriteToCSVFile("temp", ignoreTitle: true);
            if (string.IsNullOrWhiteSpace(dataFilePath))
            {
                throw new EZNEWException("Failed to generate temporary data file");
            }
            using (MySqlConnection conn = new MySqlConnection(server?.ConnectionString))
            {
                try
                {
                    dataFilePath = Path.Combine(ApplicationManager.RootPath, dataFilePath);
                    MySqlBulkLoader loader = new MySqlBulkLoader(conn)
                    {
                        Local = true,
                        TableName = dataTable.TableName,
                        FieldTerminator = CultureInfo.CurrentCulture.TextInfo.ListSeparator,
                        LineTerminator = Environment.NewLine,
                        FileName = dataFilePath,
                        NumberOfLinesToSkip = 0
                    };
                    if (bulkInsertionOptions is MySqlBulkInsertionOptions mySqlBulkInsertOptions && mySqlBulkInsertOptions != null)
                    {
                        loader.Priority = mySqlBulkInsertOptions.Priority;
                        loader.ConflictOption = mySqlBulkInsertOptions.ConflictOption;
                        loader.EscapeCharacter = mySqlBulkInsertOptions.EscapeCharacter;
                        loader.FieldQuotationOptional = mySqlBulkInsertOptions.FieldQuotationOptional;
                        loader.FieldQuotationCharacter = mySqlBulkInsertOptions.FieldQuotationCharacter;
                        loader.LineTerminator = mySqlBulkInsertOptions.LineTerminator;
                        loader.FieldTerminator = mySqlBulkInsertOptions.FieldTerminator;
                        if (!string.IsNullOrWhiteSpace(mySqlBulkInsertOptions.LinePrefix))
                        {
                            loader.LinePrefix = mySqlBulkInsertOptions.LinePrefix;
                        }
                        if (mySqlBulkInsertOptions.NumberOfLinesToSkip >= 0)
                        {
                            loader.NumberOfLinesToSkip = mySqlBulkInsertOptions.NumberOfLinesToSkip;
                        }
                        if (mySqlBulkInsertOptions.Columns.IsNullOrEmpty())
                        {
                            loader.Columns.AddRange(mySqlBulkInsertOptions.Columns);
                        }
                        if (mySqlBulkInsertOptions.Timeout > 0)
                        {
                            loader.Timeout = mySqlBulkInsertOptions.Timeout;
                        }
                        if (!string.IsNullOrWhiteSpace(mySqlBulkInsertOptions.CharacterSet))
                        {
                            loader.CharacterSet = mySqlBulkInsertOptions.CharacterSet;
                        }
                    }
                    if (loader.Columns.IsNullOrEmpty())
                    {
                        loader.Columns.AddRange(dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
                    }
                    conn.Open();
                    await loader.LoadAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (conn != null && conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                    if (File.Exists(dataFilePath))
                    {
                        File.Delete(dataFilePath);
                    }
                }
            }
        }

        #endregion
    }
}
