using System;
using System.Collections.Generic;
using System.Text;
using EZNEW.Data.Conversion;
using EZNEW.Exceptions;

namespace EZNEW.Data.MySQL
{
    /// <summary>
    /// Defines default field converter for mysql
    /// </summary>
    internal class MySqlDefaultFieldConverter : IFieldConverter
    {
        public FieldConversionResult Convert(FieldConversionContext fieldConversionContext)
        {
            if (string.IsNullOrWhiteSpace(fieldConversionContext?.ConversionName))
            {
                return null;
            }
            string formatedFieldName;
            switch (fieldConversionContext.ConversionName)
            {
                case FieldConversionNames.StringLength:
                    formatedFieldName = string.IsNullOrWhiteSpace(fieldConversionContext.ObjectName)
                        ? $"CHAR_LENGTH({fieldConversionContext.ObjectName}.{MySqlManager.WrapKeyword(fieldConversionContext.FieldName)})"
                        : $"CHAR_LENGTH({MySqlManager.WrapKeyword(fieldConversionContext.FieldName)})";
                    break;
                default:
                    throw new EZNEWException($"{MySqlManager.CurrentDatabaseServerType} does not support field conversion: {fieldConversionContext.ConversionName}");
            }
            return new FieldConversionResult()
            {
                NewFieldName = formatedFieldName
            };
        }
    }
}
