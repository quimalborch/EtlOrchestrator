using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using EtlOrchestrator.Infrastructure.Extensions;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Connectors
{
    /// <summary>
    /// Implementación de ITransform para filtrar registros según condiciones configurables
    /// </summary>
    public class DataFilterTransform : ITransform
    {
        private readonly ILogger<DataFilterTransform> _logger;
        private readonly Dictionary<string, Func<object, object, bool>> _filterOperations;

        public DataFilterTransform(ILogger<DataFilterTransform> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            // Inicializar operaciones de filtrado predefinidas
            _filterOperations = new Dictionary<string, Func<object, object, bool>>
            {
                // Operadores de igualdad
                { "Equals", (value, compareValue) => 
                    ObjectsEqual(value, compareValue) },
                { "NotEquals", (value, compareValue) => 
                    !ObjectsEqual(value, compareValue) },
                
                // Operadores de comparación numérica
                { "GreaterThan", (value, compareValue) => 
                    TryCompareNumerics(value, compareValue, (v1, v2) => v1 > v2) },
                { "GreaterThanOrEqual", (value, compareValue) => 
                    TryCompareNumerics(value, compareValue, (v1, v2) => v1 >= v2) },
                { "LessThan", (value, compareValue) => 
                    TryCompareNumerics(value, compareValue, (v1, v2) => v1 < v2) },
                { "LessThanOrEqual", (value, compareValue) => 
                    TryCompareNumerics(value, compareValue, (v1, v2) => v1 <= v2) },
                
                // Operadores de texto
                { "Contains", (value, compareValue) => 
                    value != null && compareValue != null && 
                    value.ToString().Contains(compareValue.ToString()) },
                { "StartsWith", (value, compareValue) => 
                    value != null && compareValue != null && 
                    value.ToString().StartsWith(compareValue.ToString()) },
                { "EndsWith", (value, compareValue) => 
                    value != null && compareValue != null && 
                    value.ToString().EndsWith(compareValue.ToString()) },
                { "Matches", (value, compareValue) => 
                    value != null && compareValue != null && 
                    Regex.IsMatch(value.ToString(), compareValue.ToString()) },
                
                // Operadores de colección
                { "In", (value, compareValue) => 
                    value != null && compareValue is IEnumerable<object> list && 
                    list.Any(item => ObjectsEqual(value, item)) },
                { "NotIn", (value, compareValue) => 
                    value != null && compareValue is IEnumerable<object> list && 
                    !list.Any(item => ObjectsEqual(value, item)) },
                
                // Operadores para valores nulos
                { "IsNull", (value, _) => value == null },
                { "IsNotNull", (value, _) => value != null },
                
                // Operadores de fecha/hora
                { "DateBefore", (value, compareValue) => 
                    TryCompareDates(value, compareValue, (d1, d2) => d1 < d2) },
                { "DateAfter", (value, compareValue) => 
                    TryCompareDates(value, compareValue, (d1, d2) => d1 > d2) },
                { "DateEquals", (value, compareValue) => 
                    TryCompareDates(value, compareValue, (d1, d2) => d1.Date == d2.Date) },
            };
        }

        /// <summary>
        /// Filtra los registros según las condiciones especificadas en los metadatos
        /// </summary>
        public async Task<IEnumerable<Record>> TransformAsync(IEnumerable<Record> records)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            var recordsList = records.ToList();
            _logger.LogInformation("Iniciando filtrado de {Count} registros", recordsList.Count);

            // Verificar si hay configuración de filtrado
            var firstRecord = recordsList.FirstOrDefault();
            if (firstRecord == null || !firstRecord.Metadata.ContainsKey("FilterConfig"))
            {
                _logger.LogWarning("No se encontró configuración de filtrado en los metadatos");
                return recordsList;
            }

            try
            {
                // Obtener configuración de filtrado
                var filterConfig = firstRecord.Metadata["FilterConfig"] as Dictionary<string, object>;
                if (filterConfig == null)
                {
                    _logger.LogWarning("El formato de configuración de filtrado es inválido");
                    return recordsList;
                }

                // Obtener condiciones de filtrado
                var filterConditions = filterConfig.ContainsKey("Conditions")
                    ? (filterConfig["Conditions"] as IEnumerable<Dictionary<string, object>>)?.ToList() 
                    : null;

                if (filterConditions == null || filterConditions.Count == 0)
                {
                    _logger.LogWarning("No se encontraron condiciones de filtrado válidas");
                    return recordsList;
                }

                // Obtener tipo de lógica (AND/OR)
                var logicType = filterConfig.ContainsKey("LogicType") && filterConfig["LogicType"] is string type
                    ? type
                    : "AND";

                // Realizar el filtrado
                var result = await Task.Run(() => FilterRecords(recordsList, filterConditions, logicType));
                
                _logger.LogInformation("Filtrado completado. Se mantuvieron {Count} de {Total} registros", 
                    result.Count(), recordsList.Count);
                
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error durante el filtrado de datos: {Message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Filtra los registros según las condiciones especificadas
        /// </summary>
        private IEnumerable<Record> FilterRecords(
            List<Record> records,
            List<Dictionary<string, object>> filterConditions,
            string logicType)
        {
            return records.Where(record => EvaluateConditions(record, filterConditions, logicType));
        }

        /// <summary>
        /// Evalúa si un registro cumple con las condiciones según la lógica indicada
        /// </summary>
        private bool EvaluateConditions(
            Record record,
            List<Dictionary<string, object>> conditions,
            string logicType)
        {
            if (logicType.Equals("OR", StringComparison.OrdinalIgnoreCase))
            {
                // Con lógica OR, al menos una condición debe cumplirse
                return conditions.Any(condition => EvaluateCondition(record, condition));
            }
            else
            {
                // Con lógica AND (por defecto), todas las condiciones deben cumplirse
                return conditions.All(condition => EvaluateCondition(record, condition));
            }
        }

        /// <summary>
        /// Evalúa si un registro cumple con una condición específica
        /// </summary>
        private bool EvaluateCondition(Record record, Dictionary<string, object> condition)
        {
            if (!condition.TryGetValue("Field", out var fieldObj) || !(fieldObj is string field))
                return false;

            if (!condition.TryGetValue("Operator", out var operatorObj) || !(operatorObj is string op))
                return false;

            condition.TryGetValue("Value", out var compareValue);

            // Negación
            bool negate = false;
            if (condition.TryGetValue("Negate", out var negateObj) && negateObj is bool negateValue)
            {
                negate = negateValue;
            }

            // Obtener el valor del campo del registro
            var fieldValue = record.HasProperty(field) ? record.GetProperty(field) : null;

            // Ejecutar operación de filtrado
            if (_filterOperations.TryGetValue(op, out var operation))
            {
                bool result = operation(fieldValue, compareValue);
                return negate ? !result : result;
            }

            _logger.LogWarning("Operador de filtrado no reconocido: {Operator}", op);
            return false;
        }

        /// <summary>
        /// Compara si dos objetos son iguales, manejando diferentes tipos
        /// </summary>
        private bool ObjectsEqual(object value1, object value2)
        {
            if (value1 == null && value2 == null)
                return true;
                
            if (value1 == null || value2 == null)
                return false;
                
            // Si son del mismo tipo, usar Equals
            if (value1.GetType() == value2.GetType())
                return value1.Equals(value2);
                
            // Intentar convertir a decimal para comparación numérica
            if (decimal.TryParse(value1.ToString(), out var num1) && 
                decimal.TryParse(value2.ToString(), out var num2))
            {
                return num1 == num2;
            }
                
            // Comparar como strings en último caso
            return value1.ToString() == value2.ToString();
        }

        /// <summary>
        /// Intenta comparar dos valores como números usando la función especificada
        /// </summary>
        private bool TryCompareNumerics(object value1, object value2, Func<decimal, decimal, bool> comparison)
        {
            if (value1 == null || value2 == null)
                return false;
                
            if (decimal.TryParse(value1.ToString(), out var num1) && 
                decimal.TryParse(value2.ToString(), out var num2))
            {
                return comparison(num1, num2);
            }
                
            return false;
        }

        /// <summary>
        /// Intenta comparar dos valores como fechas usando la función especificada
        /// </summary>
        private bool TryCompareDates(object value1, object value2, Func<DateTime, DateTime, bool> comparison)
        {
            if (value1 == null || value2 == null)
                return false;
                
            if (DateTime.TryParse(value1.ToString(), out var date1) && 
                DateTime.TryParse(value2.ToString(), out var date2))
            {
                return comparison(date1, date2);
            }
                
            return false;
        }

        /// <summary>
        /// Agrega una operación de filtrado personalizada
        /// </summary>
        public void AddFilterOperation(string name, Func<object, object, bool> operation)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));
                
            if (operation == null)
                throw new ArgumentNullException(nameof(operation));
                
            _filterOperations[name] = operation;
        }
    }
} 