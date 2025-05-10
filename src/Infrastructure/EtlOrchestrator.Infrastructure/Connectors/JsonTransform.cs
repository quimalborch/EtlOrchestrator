using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using EtlOrchestrator.Infrastructure.Extensions;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Connectors
{
    /// <summary>
    /// Implementación de ITransform para manipular datos JSON
    /// </summary>
    public class JsonTransform : ITransform
    {
        private readonly ILogger<JsonTransform> _logger;
        private readonly JsonSerializerOptions _jsonOptions;

        public JsonTransform(ILogger<JsonTransform> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            // Opciones por defecto para serialización/deserialización JSON
            _jsonOptions = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                WriteIndented = true,
                AllowTrailingCommas = true,
                ReadCommentHandling = JsonCommentHandling.Skip
            };
        }

        /// <summary>
        /// Transforma registros aplicando operaciones JSON según la configuración en metadatos
        /// </summary>
        public async Task<IEnumerable<Record>> TransformAsync(IEnumerable<Record> records)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            var recordsList = records.ToList();
            _logger.LogInformation("Iniciando transformación JSON para {Count} registros", recordsList.Count);

            // Verificar si hay configuración de JSON
            var firstRecord = recordsList.FirstOrDefault();
            if (firstRecord == null || !firstRecord.Metadata.ContainsKey("JsonConfig"))
            {
                _logger.LogWarning("No se encontró configuración JSON en los metadatos");
                return recordsList;
            }

            try
            {
                // Obtener configuración JSON
                var jsonConfig = firstRecord.Metadata["JsonConfig"] as Dictionary<string, object>;
                if (jsonConfig == null)
                {
                    _logger.LogWarning("El formato de configuración JSON es inválido");
                    return recordsList;
                }

                // Obtener operación a realizar
                if (!jsonConfig.TryGetValue("Operation", out var operationObj) || !(operationObj is string operation))
                {
                    _logger.LogWarning("Operación JSON no especificada");
                    return recordsList;
                }

                var transformedRecords = new List<Record>();

                // Ejecutar la operación correspondiente
                await Task.Run(() =>
                {
                    foreach (var record in recordsList)
                    {
                        try
                        {
                            Record transformedRecord = null;

                            switch (operation.ToLower())
                            {
                                case "parse":
                                    transformedRecord = ParseJsonRecord(record, jsonConfig);
                                    break;
                                case "serialize":
                                    transformedRecord = SerializeToJsonRecord(record, jsonConfig);
                                    break;
                                case "extract":
                                    transformedRecord = ExtractJsonPathValue(record, jsonConfig);
                                    break;
                                case "flatten":
                                    transformedRecord = FlattenJsonRecord(record, jsonConfig);
                                    break;
                                default:
                                    _logger.LogWarning("Operación JSON no soportada: {Operation}", operation);
                                    transformedRecord = record;
                                    break;
                            }

                            if (transformedRecord != null)
                            {
                                transformedRecords.Add(transformedRecord);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Error procesando registro JSON: {Message}", ex.Message);
                            
                            // Determinar si incluir registros con error
                            bool includeOnError = jsonConfig.TryGetValue("IncludeOnError", out var includeObj) &&
                                                includeObj is bool includeValue && includeValue;
                                                
                            if (includeOnError)
                            {
                                // Marcar el registro con error y agregarlo
                                record.Metadata["JsonError"] = ex.Message;
                                transformedRecords.Add(record);
                            }
                        }
                    }
                });

                _logger.LogInformation("Transformación JSON completada. Resultaron {Count} registros", transformedRecords.Count);
                return transformedRecords;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error durante la transformación JSON: {Message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Analiza un campo JSON y lo convierte en propiedades del registro
        /// </summary>
        private Record ParseJsonRecord(Record record, Dictionary<string, object> config)
        {
            // Obtener campo que contiene el JSON
            if (!config.TryGetValue("SourceField", out var sourceFieldObj) || !(sourceFieldObj is string sourceField))
            {
                throw new ArgumentException("Campo fuente JSON no especificado");
            }

            // Verificar si el registro tiene la propiedad
            if (!record.HasProperty(sourceField))
            {
                throw new ArgumentException($"El registro no contiene el campo JSON '{sourceField}'");
            }

            // Obtener valor JSON
            var jsonValue = record.GetProperty(sourceField)?.ToString();
            if (string.IsNullOrEmpty(jsonValue))
            {
                throw new ArgumentException($"El valor JSON en '{sourceField}' es nulo o vacío");
            }

            // Crear nuevo registro con mismos metadatos
            var result = new Record();
            foreach (var meta in record.Metadata)
            {
                result.Metadata[meta.Key] = meta.Value;
            }

            // Deserializar JSON a diccionario
            var jsonDict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(jsonValue, _jsonOptions);
            
            // Copiar propiedades existentes excepto la fuente JSON
            foreach (var property in record.GetProperties())
            {
                if (property.Key != sourceField)
                {
                    result.SetProperty(property.Key, property.Value);
                }
            }

            // Agregar propiedades desde JSON
            foreach (var kvp in jsonDict)
            {
                var value = ExtractTypedValueFromJsonElement(kvp.Value);
                result.SetProperty(kvp.Key, value);
            }

            // Agregar metadatos sobre la operación
            result.Metadata["JsonParsed"] = true;
            result.Metadata["JsonSourceField"] = sourceField;

            return result;
        }

        /// <summary>
        /// Serializa propiedades del registro a un campo JSON
        /// </summary>
        private Record SerializeToJsonRecord(Record record, Dictionary<string, object> config)
        {
            // Obtener campo destino para el JSON
            if (!config.TryGetValue("TargetField", out var targetFieldObj) || !(targetFieldObj is string targetField))
            {
                throw new ArgumentException("Campo destino JSON no especificado");
            }

            // Obtener campos a incluir (opcional)
            string[] fieldsToInclude = null;
            if (config.TryGetValue("IncludeFields", out var includeFieldsObj) && includeFieldsObj is IEnumerable<string> fields)
            {
                fieldsToInclude = fields.ToArray();
            }

            // Crear nuevo registro con mismos metadatos
            var result = new Record();
            foreach (var meta in record.Metadata)
            {
                result.Metadata[meta.Key] = meta.Value;
            }

            // Copiar todas las propiedades
            foreach (var property in record.GetProperties())
            {
                result.SetProperty(property.Key, property.Value);
            }

            // Crear diccionario con los campos a serializar
            var jsonDict = new Dictionary<string, object>();
            var properties = record.GetProperties();
            
            foreach (var property in properties)
            {
                // Si hay lista de inclusión, verificar si el campo está en ella
                if (fieldsToInclude != null && !fieldsToInclude.Contains(property.Key))
                    continue;
                    
                jsonDict[property.Key] = property.Value;
            }

            // Serializar a JSON
            var jsonString = JsonSerializer.Serialize(jsonDict, _jsonOptions);
            
            // Guardar en el campo destino
            result.SetProperty(targetField, jsonString);

            // Agregar metadatos sobre la operación
            result.Metadata["JsonSerialized"] = true;
            result.Metadata["JsonTargetField"] = targetField;

            return result;
        }

        /// <summary>
        /// Extrae valores de un JSON usando expresiones de ruta
        /// </summary>
        private Record ExtractJsonPathValue(Record record, Dictionary<string, object> config)
        {
            // Obtener campo que contiene el JSON
            if (!config.TryGetValue("SourceField", out var sourceFieldObj) || !(sourceFieldObj is string sourceField))
            {
                throw new ArgumentException("Campo fuente JSON no especificado");
            }

            // Obtener mapeo de rutas JSON a campos destino
            if (!config.TryGetValue("PathMapping", out var pathMappingObj) || 
                !(pathMappingObj is Dictionary<string, string> pathMapping) ||
                pathMapping.Count == 0)
            {
                throw new ArgumentException("Mapeo de rutas JSON no especificado");
            }

            // Verificar si el registro tiene la propiedad
            if (!record.HasProperty(sourceField))
            {
                throw new ArgumentException($"El registro no contiene el campo JSON '{sourceField}'");
            }

            // Obtener valor JSON
            var jsonValue = record.GetProperty(sourceField)?.ToString();
            if (string.IsNullOrEmpty(jsonValue))
            {
                throw new ArgumentException($"El valor JSON en '{sourceField}' es nulo o vacío");
            }

            // Crear nuevo registro con mismos metadatos
            var result = new Record();
            foreach (var meta in record.Metadata)
            {
                result.Metadata[meta.Key] = meta.Value;
            }

            // Copiar propiedades existentes
            foreach (var property in record.GetProperties())
            {
                result.SetProperty(property.Key, property.Value);
            }

            // Deserializar JSON a elemento raíz
            var jsonDocument = JsonDocument.Parse(jsonValue);
            var root = jsonDocument.RootElement;

            // Extraer valores según las rutas especificadas
            foreach (var mapping in pathMapping)
            {
                var jsonPath = mapping.Key;
                var targetField = mapping.Value;
                
                try
                {
                    var value = ExtractValueFromJsonPath(root, jsonPath);
                    if (value != null)
                    {
                        result.SetProperty(targetField, value);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error extrayendo ruta JSON '{JsonPath}': {Message}", jsonPath, ex.Message);
                }
            }

            // Agregar metadatos sobre la operación
            result.Metadata["JsonExtracted"] = true;
            
            return result;
        }

        /// <summary>
        /// Aplana un objeto JSON en propiedades de nivel único
        /// </summary>
        private Record FlattenJsonRecord(Record record, Dictionary<string, object> config)
        {
            // Obtener campo que contiene el JSON
            if (!config.TryGetValue("SourceField", out var sourceFieldObj) || !(sourceFieldObj is string sourceField))
            {
                throw new ArgumentException("Campo fuente JSON no especificado");
            }

            // Obtener separador para nombres de campos (opcional)
            string separator = "_";
            if (config.TryGetValue("Separator", out var separatorObj) && separatorObj is string separatorValue)
            {
                separator = separatorValue;
            }

            // Verificar si el registro tiene la propiedad
            if (!record.HasProperty(sourceField))
            {
                throw new ArgumentException($"El registro no contiene el campo JSON '{sourceField}'");
            }

            // Obtener valor JSON
            var jsonValue = record.GetProperty(sourceField)?.ToString();
            if (string.IsNullOrEmpty(jsonValue))
            {
                throw new ArgumentException($"El valor JSON en '{sourceField}' es nulo o vacío");
            }

            // Crear nuevo registro con mismos metadatos
            var result = new Record();
            foreach (var meta in record.Metadata)
            {
                result.Metadata[meta.Key] = meta.Value;
            }

            // Copiar propiedades existentes excepto la fuente JSON
            foreach (var property in record.GetProperties())
            {
                if (property.Key != sourceField)
                {
                    result.SetProperty(property.Key, property.Value);
                }
            }

            // Deserializar JSON a elemento
            var jsonDocument = JsonDocument.Parse(jsonValue);
            var root = jsonDocument.RootElement;

            // Aplanar el objeto JSON
            var flattenedProperties = new Dictionary<string, object>();
            FlattenJsonElement(root, "", separator, flattenedProperties);

            // Agregar propiedades aplanadas
            foreach (var prop in flattenedProperties)
            {
                result.SetProperty(prop.Key, prop.Value);
            }

            // Agregar metadatos sobre la operación
            result.Metadata["JsonFlattened"] = true;
            result.Metadata["JsonSourceField"] = sourceField;

            return result;
        }

        /// <summary>
        /// Extrae un valor tipado de un elemento JSON
        /// </summary>
        private object ExtractTypedValueFromJsonElement(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String:
                    return element.GetString();
                case JsonValueKind.Number:
                    if (element.TryGetInt32(out int intValue))
                        return intValue;
                    if (element.TryGetInt64(out long longValue))
                        return longValue;
                    return element.GetDecimal();
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.Null:
                    return null;
                case JsonValueKind.Object:
                    return element.ToString(); // Serializar objeto anidado como string
                case JsonValueKind.Array:
                    return element.ToString(); // Serializar array como string
                default:
                    return element.ToString();
            }
        }

        /// <summary>
        /// Extrae un valor de un elemento JSON usando una expresión de ruta
        /// </summary>
        private object ExtractValueFromJsonPath(JsonElement root, string jsonPath)
        {
            // Implementación simple de JSONPath, soporta solo rutas delimitadas por puntos
            var segments = jsonPath.Split('.');
            JsonElement current = root;

            foreach (var segment in segments)
            {
                // Manejar acceso a elementos de array: path[index]
                if (segment.Contains("[") && segment.EndsWith("]"))
                {
                    var arraySegments = segment.Split('[');
                    var propertyName = arraySegments[0];
                    var indexStr = arraySegments[1].TrimEnd(']');
                    
                    if (!int.TryParse(indexStr, out var index))
                    {
                        throw new ArgumentException($"Índice de array inválido: {indexStr}");
                    }
                    
                    // Acceder a la propiedad, luego al índice del array
                    if (current.ValueKind == JsonValueKind.Object && current.TryGetProperty(propertyName, out var arrayProp))
                    {
                        if (arrayProp.ValueKind == JsonValueKind.Array && index < arrayProp.GetArrayLength())
                        {
                            current = arrayProp[index];
                        }
                        else
                        {
                            throw new ArgumentException($"No se pudo acceder al índice {index} en el array");
                        }
                    }
                    else
                    {
                        throw new ArgumentException($"Propiedad no encontrada: {propertyName}");
                    }
                }
                // Acceso a propiedad normal
                else if (current.ValueKind == JsonValueKind.Object && current.TryGetProperty(segment, out var property))
                {
                    current = property;
                }
                else
                {
                    throw new ArgumentException($"Propiedad no encontrada: {segment}");
                }
            }
            
            return ExtractTypedValueFromJsonElement(current);
        }

        /// <summary>
        /// Aplana un elemento JSON en un diccionario de propiedades
        /// </summary>
        private void FlattenJsonElement(
            JsonElement element, 
            string prefix, 
            string separator, 
            Dictionary<string, object> result)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    foreach (var property in element.EnumerateObject())
                    {
                        string newPrefix = string.IsNullOrEmpty(prefix) 
                            ? property.Name 
                            : $"{prefix}{separator}{property.Name}";
                            
                        FlattenJsonElement(property.Value, newPrefix, separator, result);
                    }
                    break;
                
                case JsonValueKind.Array:
                    int index = 0;
                    foreach (var item in element.EnumerateArray())
                    {
                        string newPrefix = $"{prefix}{separator}{index}";
                        FlattenJsonElement(item, newPrefix, separator, result);
                        index++;
                    }
                    break;
                
                default:
                    // Para valores escalares, agregar directamente
                    result[prefix] = ExtractTypedValueFromJsonElement(element);
                    break;
            }
        }

        /// <summary>
        /// Configura las opciones de serialización/deserialización JSON
        /// </summary>
        public void ConfigureJsonOptions(Action<JsonSerializerOptions> configureOptions)
        {
            if (configureOptions == null)
                throw new ArgumentNullException(nameof(configureOptions));
                
            configureOptions(_jsonOptions);
        }
    }
} 