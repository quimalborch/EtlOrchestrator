using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using EtlOrchestrator.Infrastructure.Extensions;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Connectors
{
    /// <summary>
    /// Implementación de ITransform para agregar datos según criterios configurables
    /// </summary>
    public class DataAggregatorTransform : ITransform
    {
        private readonly ILogger<DataAggregatorTransform> _logger;
        private readonly Dictionary<string, Func<IEnumerable<object>, object>> _aggregationFunctions;

        public DataAggregatorTransform(ILogger<DataAggregatorTransform> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            // Inicializar funciones de agregación predefinidas
            _aggregationFunctions = new Dictionary<string, Func<IEnumerable<object>, object>>
            {
                { "Sum", values => values
                    .Where(v => v != null && decimal.TryParse(v.ToString(), out _))
                    .Select(v => Convert.ToDecimal(v))
                    .Sum() },
                { "Average", values => values
                    .Where(v => v != null && decimal.TryParse(v.ToString(), out _))
                    .Select(v => Convert.ToDecimal(v))
                    .DefaultIfEmpty(0)
                    .Average() },
                { "Min", values => values
                    .Where(v => v != null && decimal.TryParse(v.ToString(), out _))
                    .Select(v => Convert.ToDecimal(v))
                    .DefaultIfEmpty(0)
                    .Min() },
                { "Max", values => values
                    .Where(v => v != null && decimal.TryParse(v.ToString(), out _))
                    .Select(v => Convert.ToDecimal(v))
                    .DefaultIfEmpty(0)
                    .Max() },
                { "Count", values => values.Count() },
                { "CountDistinct", values => values.Distinct().Count() },
                { "Concatenate", values => string.Join(",", values
                    .Where(v => v != null)
                    .Select(v => v.ToString())) },
                { "First", values => values.FirstOrDefault() },
                { "Last", values => values.LastOrDefault() },
            };
        }

        /// <summary>
        /// Agrega los registros según las configuraciones especificadas en los metadatos
        /// </summary>
        public async Task<IEnumerable<Record>> TransformAsync(IEnumerable<Record> records)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            var recordsList = records.ToList();
            _logger.LogInformation("Iniciando agregación de {Count} registros", recordsList.Count);

            // Verificar si hay configuración de agregación
            var firstRecord = recordsList.FirstOrDefault();
            if (firstRecord == null || !firstRecord.Metadata.ContainsKey("AggregationConfig"))
            {
                _logger.LogWarning("No se encontró configuración de agregación en los metadatos");
                return recordsList;
            }

            try
            {
                // Obtener configuración de agregación
                var aggregationConfig = firstRecord.Metadata["AggregationConfig"] as Dictionary<string, object>;
                if (aggregationConfig == null)
                {
                    _logger.LogWarning("El formato de configuración de agregación es inválido");
                    return recordsList;
                }

                // Obtener campos de agrupación
                var groupByFields = aggregationConfig.ContainsKey("GroupByFields")
                    ? (aggregationConfig["GroupByFields"] as IEnumerable<string>)?.ToList() ?? new List<string>()
                    : new List<string>();

                // Obtener operaciones de agregación
                var aggregationOperations = aggregationConfig.ContainsKey("AggregationOperations")
                    ? (aggregationConfig["AggregationOperations"] as Dictionary<string, string>) ?? new Dictionary<string, string>()
                    : new Dictionary<string, string>();

                // Realizar la agregación
                var result = await Task.Run(() => AggregateRecords(recordsList, groupByFields, aggregationOperations));
                
                _logger.LogInformation("Agregación completada. Resultaron {Count} registros agregados", result.Count());
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error durante la agregación de datos: {Message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Realiza la agregación de registros según los campos y operaciones especificadas
        /// </summary>
        private IEnumerable<Record> AggregateRecords(
            List<Record> records, 
            List<string> groupByFields, 
            Dictionary<string, string> aggregationOperations)
        {
            // Si no hay campos de agrupación, realizar agregación global
            if (groupByFields.Count == 0)
            {
                _logger.LogInformation("Realizando agregación global (sin agrupación)");
                return new[] { CreateAggregatedRecord(records, null, aggregationOperations) };
            }

            _logger.LogInformation("Realizando agregación agrupada por {Fields}", string.Join(", ", groupByFields));
            
            // Agrupar registros según los campos especificados
            var groups = records.GroupBy(record => 
                string.Join("||", groupByFields.Select(field => 
                    record.HasProperty(field) ? record.GetProperty(field)?.ToString() ?? "null" : "undefined")));

            // Crear un registro agregado para cada grupo
            return groups.Select(group => 
                CreateAggregatedRecord(group.ToList(), groupByFields, aggregationOperations));
        }

        /// <summary>
        /// Crea un registro agregado a partir de un grupo de registros
        /// </summary>
        private Record CreateAggregatedRecord(
            List<Record> groupRecords, 
            List<string> groupByFields, 
            Dictionary<string, string> aggregationOperations)
        {
            var result = new Record();
            
            // Copiar los campos de agrupación del primer registro
            if (groupByFields != null && groupByFields.Count > 0 && groupRecords.Count > 0)
            {
                foreach (var field in groupByFields)
                {
                    if (groupRecords[0].HasProperty(field))
                    {
                        result.SetProperty(field, groupRecords[0].GetProperty(field));
                    }
                }
            }
            
            // Aplicar operaciones de agregación
            foreach (var op in aggregationOperations)
            {
                var fieldName = op.Key;
                var aggregationType = op.Value;
                
                if (_aggregationFunctions.TryGetValue(aggregationType, out var aggregateFunc))
                {
                    // Extraer valores para el campo de todos los registros del grupo
                    var values = groupRecords
                        .Where(r => r.HasProperty(fieldName))
                        .Select(r => r.GetProperty(fieldName))
                        .ToList();
                    
                    // Aplicar función de agregación
                    var aggregatedValue = aggregateFunc(values);
                    
                    // Guardar resultado con nombre descriptivo
                    string resultFieldName = $"{aggregationType}_{fieldName}";
                    result.SetProperty(resultFieldName, aggregatedValue);
                }
            }
            
            // Transferir metadatos del primer registro
            if (groupRecords.Count > 0)
            {
                foreach (var meta in groupRecords[0].Metadata)
                {
                    // No transferir la configuración de agregación
                    if (meta.Key != "AggregationConfig")
                    {
                        result.Metadata[meta.Key] = meta.Value;
                    }
                }
            }
            
            // Agregar metadatos sobre la agregación
            result.Metadata["AggregationApplied"] = true;
            result.Metadata["RecordsAggregated"] = groupRecords.Count;
            
            return result;
        }

        /// <summary>
        /// Agrega una función de agregación personalizada
        /// </summary>
        public void AddAggregationFunction(string name, Func<IEnumerable<object>, object> function)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));
                
            if (function == null)
                throw new ArgumentNullException(nameof(function));
            
            _aggregationFunctions[name] = function;
        }
    }
} 