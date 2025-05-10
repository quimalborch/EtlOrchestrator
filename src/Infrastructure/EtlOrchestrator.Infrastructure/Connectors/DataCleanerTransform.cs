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
    /// Implementación de ITransform para limpieza de datos
    /// </summary>
    public class DataCleanerTransform : ITransform
    {
        private readonly ILogger<DataCleanerTransform> _logger;
        
        // Reglas de validación y transformación predefinidas
        private readonly Dictionary<string, Func<object, bool>> _validationRules;
        private readonly Dictionary<string, Func<object, object>> _transformationRules;

        public DataCleanerTransform(ILogger<DataCleanerTransform> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            // Inicializar reglas de validación
            _validationRules = new Dictionary<string, Func<object, bool>>
            {
                { "NotNull", value => value != null },
                { "NotEmpty", value => value != null && value.ToString().Trim().Length > 0 },
                { "IsNumeric", value => value != null && decimal.TryParse(value.ToString(), out _) },
                { "IsEmail", value => value != null && Regex.IsMatch(value.ToString(), @"^[^@\s]+@[^@\s]+\.[^@\s]+$") },
                { "IsDate", value => value != null && DateTime.TryParse(value.ToString(), out _) }
            };
            
            // Inicializar reglas de transformación
            _transformationRules = new Dictionary<string, Func<object, object>>
            {
                { "Trim", value => value?.ToString()?.Trim() },
                { "ToUpper", value => value?.ToString()?.ToUpper() },
                { "ToLower", value => value?.ToString()?.ToLower() },
                { "RemoveSpecialChars", value => value != null ? Regex.Replace(value.ToString(), "[^a-zA-Z0-9]", "") : null },
                { "ParseToDecimal", value => value != null && decimal.TryParse(value.ToString(), out var result) ? result : null },
                { "ParseToInt", value => value != null && int.TryParse(value.ToString(), out var result) ? result : null },
                { "ParseToDate", value => value != null && DateTime.TryParse(value.ToString(), out var result) ? result : null }
            };
        }

        /// <summary>
        /// Aplica transformaciones y validaciones a los registros
        /// </summary>
        /// <param name="records">Registros a transformar</param>
        /// <returns>Registros transformados</returns>
        public async Task<IEnumerable<Record>> TransformAsync(IEnumerable<Record> records)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            _logger.LogInformation("Iniciando transformación de datos para {Count} registros", records.Count());
            
            var transformedRecords = new List<Record>();
            var recordsArray = records.ToArray();
            
            // Procesamos los registros de forma asíncrona para permitir operaciones costosas
            await Task.Run(() => 
            {
                foreach (var record in recordsArray)
                {
                    try
                    {
                        var transformedRecord = ApplyTransformations(record);
                        
                        if (transformedRecord != null)
                        {
                            transformedRecords.Add(transformedRecord);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Error al transformar registro: {Message}", ex.Message);
                    }
                }
            });
            
            _logger.LogInformation("Transformación completada. Se procesaron {Total} registros, resultando en {Valid} registros válidos", 
                recordsArray.Length, transformedRecords.Count);
            
            return transformedRecords;
        }

        /// <summary>
        /// Aplica reglas de transformación y validación a un registro
        /// </summary>
        private Record ApplyTransformations(Record record)
        {
            if (record == null)
                return null;
            
            var transformedRecord = new Record();
            var properties = record.GetProperties();
            var validationErrors = new List<string>();
            
            // Transferir metadatos
            foreach (var meta in record.Metadata)
            {
                transformedRecord.Metadata[meta.Key] = meta.Value;
            }
            
            // Procesar cada propiedad
            foreach (var property in properties)
            {
                string key = property.Key;
                object value = property.Value;
                
                // Aplicar transformaciones si están configuradas en los metadatos
                if (record.Metadata.TryGetValue($"{key}_Transformations", out var transformationsObj) && 
                    transformationsObj is IEnumerable<string> transformations)
                {
                    foreach (var transformation in transformations)
                    {
                        if (_transformationRules.TryGetValue(transformation, out var transformFunc))
                        {
                            value = transformFunc(value);
                        }
                    }
                }
                
                // Aplicar validaciones si están configuradas en los metadatos
                if (record.Metadata.TryGetValue($"{key}_Validations", out var validationsObj) && 
                    validationsObj is IEnumerable<string> validations)
                {
                    foreach (var validation in validations)
                    {
                        if (_validationRules.TryGetValue(validation, out var validateFunc) && !validateFunc(value))
                        {
                            validationErrors.Add($"Validación fallida para '{key}': {validation}");
                            break;
                        }
                    }
                }
                
                // Establecer el valor en el registro transformado
                transformedRecord.SetProperty(key, value);
            }
            
            // Si hay errores de validación, registrarlos en los metadatos
            if (validationErrors.Count > 0)
            {
                transformedRecord.Metadata["ValidationErrors"] = validationErrors;
                
                // Si la estrategia es excluir registros inválidos, devolver null
                if (record.Metadata.TryGetValue("OnValidationError", out var strategy) && 
                    strategy.ToString() == "Exclude")
                {
                    return null;
                }
            }
            
            return transformedRecord;
        }
        
        /// <summary>
        /// Agrega una regla de validación personalizada
        /// </summary>
        public void AddValidationRule(string name, Func<object, bool> rule)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));
                
            if (rule == null)
                throw new ArgumentNullException(nameof(rule));
                
            _validationRules[name] = rule;
        }
        
        /// <summary>
        /// Agrega una regla de transformación personalizada
        /// </summary>
        public void AddTransformationRule(string name, Func<object, object> rule)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));
                
            if (rule == null)
                throw new ArgumentNullException(nameof(rule));
                
            _transformationRules[name] = rule;
        }
    }
} 