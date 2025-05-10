using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Connectors
{
    /// <summary>
    /// Implementación de ITransform para normalizar valores numéricos y textuales
    /// </summary>
    public class DataNormalizerTransform : ITransform
    {
        private readonly ILogger<DataNormalizerTransform> _logger;
        private readonly Dictionary<string, Func<IEnumerable<object>, object, object>> _normalizationFunctions;

        public DataNormalizerTransform(ILogger<DataNormalizerTransform> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            // Inicializar funciones de normalización predefinidas
            _normalizationFunctions = new Dictionary<string, Func<IEnumerable<object>, object, object>>
            {
                // Normalización Min-Max (escala valores al rango [0,1])
                { "MinMax", (values, value) => {
                    if (value == null) return null;
                    
                    var numerics = values
                        .Where(v => v != null && decimal.TryParse(v.ToString(), out _))
                        .Select(v => Convert.ToDecimal(v))
                        .ToList();
                    
                    if (!numerics.Any() || !decimal.TryParse(value.ToString(), out var numValue))
                        return value;
                        
                    var min = numerics.Min();
                    var max = numerics.Max();
                    
                    if (min == max) return 1.0m; // Evitar división por cero
                    
                    return (decimal)((numValue - min) / (max - min));
                }},
                
                // Normalización Z-Score (estandariza valores con media 0 y desviación estándar 1)
                { "ZScore", (values, value) => {
                    if (value == null) return null;
                    
                    var numerics = values
                        .Where(v => v != null && decimal.TryParse(v.ToString(), out _))
                        .Select(v => Convert.ToDecimal(v))
                        .ToList();
                    
                    if (!numerics.Any() || !decimal.TryParse(value.ToString(), out var numValue))
                        return value;
                        
                    var mean = numerics.Average();
                    var sumSquaredDiffs = numerics.Sum(v => (v - mean) * (v - mean));
                    var stdDev = (decimal)Math.Sqrt((double)(sumSquaredDiffs / numerics.Count));
                    
                    if (stdDev == 0) return 0.0m; // Evitar división por cero
                    
                    return (numValue - mean) / stdDev;
                }},
                
                // Normalización de texto (reduce variantes de texto a una forma estándar)
                { "TextStandardize", (_, value) => {
                    if (value == null) return null;
                    
                    var text = value.ToString().Trim().ToLower();
                    return text;
                }},
                
                // Normalización por porcentaje del total
                { "PercentOfTotal", (values, value) => {
                    if (value == null) return null;
                    
                    var numerics = values
                        .Where(v => v != null && decimal.TryParse(v.ToString(), out _))
                        .Select(v => Convert.ToDecimal(v))
                        .ToList();
                    
                    if (!numerics.Any() || !decimal.TryParse(value.ToString(), out var numValue))
                        return value;
                        
                    var total = numerics.Sum();
                    
                    if (total == 0) return 0.0m; // Evitar división por cero
                    
                    return (numValue / total) * 100.0m;
                }},
                
                // Normalización por rango personalizado
                { "CustomRange", (values, value) => {
                    // Esta función requiere parámetros adicionales en la configuración
                    return value;
                }},
                
                // Discretización / Agrupación en intervalos (binning)
                { "Binning", (values, value) => {
                    // Esta función requiere parámetros adicionales en la configuración
                    return value;
                }}
            };
        }

        /// <summary>
        /// Normaliza los valores de los registros según la configuración
        /// </summary>
        public async Task<IEnumerable<Record>> TransformAsync(IEnumerable<Record> records)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            var recordsList = records.ToList();
            _logger.LogInformation("Iniciando normalización de {Count} registros", recordsList.Count);

            // Verificar si hay configuración de normalización
            var firstRecord = recordsList.FirstOrDefault();
            if (firstRecord == null || !firstRecord.Metadata.ContainsKey("NormalizationConfig"))
            {
                _logger.LogWarning("No se encontró configuración de normalización en los metadatos");
                return recordsList;
            }

            try
            {
                // Obtener configuración de normalización
                var normalizeConfig = firstRecord.Metadata["NormalizationConfig"] as Dictionary<string, object>;
                if (normalizeConfig == null)
                {
                    _logger.LogWarning("El formato de configuración de normalización es inválido");
                    return recordsList;
                }

                // Obtener operaciones de normalización
                if (!normalizeConfig.TryGetValue("Operations", out var operationsObj) || 
                    !(operationsObj is Dictionary<string, Dictionary<string, object>> operations) ||
                    operations.Count == 0)
                {
                    _logger.LogWarning("No se encontraron operaciones de normalización válidas");
                    return recordsList;
                }

                // Pre-procesamiento: extraer todos los valores para cada campo a normalizar
                var fieldValues = ExtractFieldValues(recordsList, operations.Keys.ToList());

                // Normalizar cada registro
                var normalizedRecords = await Task.Run(() => 
                    NormalizeRecords(recordsList, operations, fieldValues));
                
                _logger.LogInformation("Normalización completada para {Count} registros", normalizedRecords.Count());
                return normalizedRecords;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error durante la normalización de datos: {Message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Extrae todos los valores para cada campo a normalizar
        /// </summary>
        private Dictionary<string, List<object>> ExtractFieldValues(
            List<Record> records, 
            List<string> fields)
        {
            var result = new Dictionary<string, List<object>>();
            
            foreach (var field in fields)
            {
                result[field] = records
                    .Where(r => r.HasProperty(field))
                    .Select(r => r.GetProperty(field))
                    .ToList();
            }
            
            return result;
        }

        /// <summary>
        /// Normaliza los registros según las operaciones especificadas
        /// </summary>
        private IEnumerable<Record> NormalizeRecords(
            List<Record> records,
            Dictionary<string, Dictionary<string, object>> operations,
            Dictionary<string, List<object>> fieldValues)
        {
            var result = new List<Record>();
            
            foreach (var record in records)
            {
                var normalizedRecord = new Record();
                
                // Copiar metadatos
                foreach (var meta in record.Metadata)
                {
                    normalizedRecord.Metadata[meta.Key] = meta.Value;
                }
                
                // Copiar todas las propiedades inicialmente
                foreach (var property in record.GetProperties())
                {
                    normalizedRecord.SetProperty(property.Key, property.Value);
                }
                
                // Aplicar operaciones de normalización
                foreach (var operation in operations)
                {
                    var fieldName = operation.Key;
                    var operationDetails = operation.Value;
                    
                    if (!operationDetails.TryGetValue("Type", out var typeObj) || !(typeObj is string normalizationType))
                    {
                        _logger.LogWarning("Tipo de normalización no especificado para el campo {Field}", fieldName);
                        continue;
                    }
                    
                    // Verificar si se debe crear un nuevo campo o sobrescribir el existente
                    bool createNewField = operationDetails.TryGetValue("CreateNewField", out var createNewObj) && 
                                         createNewObj is bool createNew && createNew;
                                         
                    string targetField = fieldName;
                    if (createNewField && operationDetails.TryGetValue("NewFieldName", out var newFieldObj) && 
                        newFieldObj is string newField)
                    {
                        targetField = newField;
                    }
                    
                    // Verificar si el registro tiene la propiedad
                    if (!record.HasProperty(fieldName))
                    {
                        continue;
                    }
                    
                    // Obtener valor actual
                    var value = record.GetProperty(fieldName);
                    
                    if (_normalizationFunctions.TryGetValue(normalizationType, out var normalizeFunc))
                    {
                        // Pasar parámetros adicionales para funciones que los requieren
                        if (normalizationType == "CustomRange" && 
                            operationDetails.TryGetValue("Parameters", out var paramsObj) && 
                            paramsObj is Dictionary<string, object> parameters)
                        {
                            decimal minValue = 0.0m;
                            decimal maxValue = 1.0m;
                            
                            if (parameters.TryGetValue("MinOutput", out var minObj) && 
                                decimal.TryParse(minObj.ToString(), out var min))
                            {
                                minValue = min;
                            }
                            
                            if (parameters.TryGetValue("MaxOutput", out var maxObj) && 
                                decimal.TryParse(maxObj.ToString(), out var max))
                            {
                                maxValue = max;
                            }
                            
                            // Aplicar normalización MinMax y luego escalar al rango deseado
                            var minMaxValue = _normalizationFunctions["MinMax"](fieldValues[fieldName], value);
                            
                            if (minMaxValue != null && decimal.TryParse(minMaxValue.ToString(), out var normalized))
                            {
                                var scaled = minValue + (normalized * (maxValue - minValue));
                                normalizedRecord.SetProperty(targetField, scaled);
                            }
                            else
                            {
                                normalizedRecord.SetProperty(targetField, value);
                            }
                        }
                        else if (normalizationType == "Binning" && 
                                operationDetails.TryGetValue("Parameters", out var binParamsObj) && 
                                binParamsObj is Dictionary<string, object> binParameters)
                        {
                            int numBins = 5; // Valor predeterminado
                            
                            if (binParameters.TryGetValue("NumBins", out var numBinsObj) && 
                                int.TryParse(numBinsObj.ToString(), out var bins))
                            {
                                numBins = bins;
                            }
                            
                            if (decimal.TryParse(value?.ToString(), out var numValue))
                            {
                                // Obtener valores numéricos
                                var numerics = fieldValues[fieldName]
                                    .Where(v => v != null && decimal.TryParse(v.ToString(), out _))
                                    .Select(v => Convert.ToDecimal(v))
                                    .ToList();
                                
                                if (numerics.Any())
                                {
                                    var min = numerics.Min();
                                    var max = numerics.Max();
                                    var range = max - min;
                                    
                                    if (range > 0)
                                    {
                                        var binWidth = range / numBins;
                                        var bin = (int)Math.Floor((numValue - min) / binWidth);
                                        
                                        // Asegurar que el valor esté en el rango [0, numBins-1]
                                        bin = Math.Max(0, Math.Min(bin, numBins - 1));
                                        
                                        normalizedRecord.SetProperty(targetField, bin);
                                    }
                                    else
                                    {
                                        normalizedRecord.SetProperty(targetField, 0);
                                    }
                                }
                                else
                                {
                                    normalizedRecord.SetProperty(targetField, value);
                                }
                            }
                            else
                            {
                                normalizedRecord.SetProperty(targetField, value);
                            }
                        }
                        else
                        {
                            // Aplicar normalización estándar
                            var normalizedValue = normalizeFunc(fieldValues[fieldName], value);
                            normalizedRecord.SetProperty(targetField, normalizedValue);
                        }
                    }
                    else
                    {
                        _logger.LogWarning("Tipo de normalización no soportado: {Type}", normalizationType);
                    }
                }
                
                // Agregar metadatos sobre la normalización
                normalizedRecord.Metadata["NormalizationApplied"] = true;
                
                result.Add(normalizedRecord);
            }
            
            return result;
        }

        /// <summary>
        /// Agrega una función de normalización personalizada
        /// </summary>
        public void AddNormalizationFunction(string name, Func<IEnumerable<object>, object, object> function)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));
                
            if (function == null)
                throw new ArgumentNullException(nameof(function));
                
            _normalizationFunctions[name] = function;
        }
    }
} 