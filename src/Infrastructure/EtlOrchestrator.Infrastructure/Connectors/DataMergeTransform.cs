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
    /// Implementación de ITransform para combinar registros de diferentes fuentes
    /// </summary>
    public class DataMergeTransform : ITransform
    {
        private readonly ILogger<DataMergeTransform> _logger;
        
        // Tipos de combinación soportados
        public enum MergeType
        {
            Union,      // Unión simple (concatena registros)
            Join,       // Join SQL-style basado en claves
            LeftJoin,   // Left Join SQL-style
            RightJoin,  // Right Join SQL-style
            FullJoin    // Full Outer Join SQL-style
        }

        public DataMergeTransform(ILogger<DataMergeTransform> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Combina registros según la configuración especificada en los metadatos
        /// </summary>
        public async Task<IEnumerable<Record>> TransformAsync(IEnumerable<Record> records)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            var recordsList = records.ToList();
            _logger.LogInformation("Iniciando combinación de {Count} registros", recordsList.Count);

            // Verificar si hay configuración de combinación
            var firstRecord = recordsList.FirstOrDefault();
            if (firstRecord == null || !firstRecord.Metadata.ContainsKey("MergeConfig"))
            {
                _logger.LogWarning("No se encontró configuración de combinación en los metadatos");
                return recordsList;
            }

            try
            {
                // Obtener configuración de combinación
                var mergeConfig = firstRecord.Metadata["MergeConfig"] as Dictionary<string, object>;
                if (mergeConfig == null)
                {
                    _logger.LogWarning("El formato de configuración de combinación es inválido");
                    return recordsList;
                }

                // Obtener tipo de combinación
                if (!mergeConfig.TryGetValue("MergeType", out var mergeTypeObj) || !(mergeTypeObj is string mergeTypeStr) ||
                    !Enum.TryParse<MergeType>(mergeTypeStr, true, out var mergeType))
                {
                    _logger.LogWarning("Tipo de combinación no especificado o inválido");
                    return recordsList;
                }

                // Verificar si hay varias fuentes de datos
                if (!mergeConfig.TryGetValue("DataSources", out var dataSourcesObj) || 
                    !(dataSourcesObj is Dictionary<string, List<Record>> dataSources) ||
                    dataSources.Count == 0)
                {
                    _logger.LogWarning("No se encontraron fuentes de datos para combinar");
                    return recordsList;
                }

                // Para Join, necesitamos campos de unión
                List<string> joinFields = null;
                if (mergeType != MergeType.Union)
                {
                    if (!mergeConfig.TryGetValue("JoinFields", out var joinFieldsObj) || 
                        !(joinFieldsObj is IEnumerable<string> fields) || 
                        !fields.Any())
                    {
                        _logger.LogWarning("No se especificaron campos para el join");
                        return recordsList;
                    }
                    joinFields = fields.ToList();
                }

                // Realizar la combinación
                var result = await Task.Run(() => 
                    MergeRecords(recordsList, dataSources, mergeType, joinFields));
                
                _logger.LogInformation("Combinación completada. Resultaron {Count} registros", result.Count());
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error durante la combinación de datos: {Message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Combina registros según el tipo de combinación y configuración
        /// </summary>
        private IEnumerable<Record> MergeRecords(
            List<Record> primaryRecords,
            Dictionary<string, List<Record>> dataSources,
            MergeType mergeType,
            List<string> joinFields)
        {
            // Para UNION, simplemente concatenar todos los registros
            if (mergeType == MergeType.Union)
            {
                _logger.LogInformation("Realizando combinación tipo UNION");
                
                var result = new List<Record>(primaryRecords);
                foreach (var source in dataSources)
                {
                    result.AddRange(source.Value);
                }
                
                return result;
            }
            
            // Para los diferentes tipos de JOIN
            _logger.LogInformation("Realizando combinación tipo {MergeType} en campos {Fields}", 
                mergeType, string.Join(", ", joinFields));
            
            // Determinar qué conjuntos de datos utilizar para el join
            var leftRecords = primaryRecords;
            
            // Si no hay registros secundarios, no hay nada que combinar
            if (dataSources.Count == 0)
                return leftRecords;
                
            // Tomar el primer conjunto de datos secundarios
            var rightRecords = dataSources.First().Value;
            
            // Aplicar el tipo de join correspondiente
            switch (mergeType)
            {
                case MergeType.Join:
                    return InnerJoin(leftRecords, rightRecords, joinFields);
                case MergeType.LeftJoin:
                    return LeftJoin(leftRecords, rightRecords, joinFields);
                case MergeType.RightJoin:
                    return RightJoin(leftRecords, rightRecords, joinFields);
                case MergeType.FullJoin:
                    return FullJoin(leftRecords, rightRecords, joinFields);
                default:
                    _logger.LogWarning("Tipo de combinación no implementado: {MergeType}", mergeType);
                    return leftRecords;
            }
        }

        /// <summary>
        /// Realiza un INNER JOIN entre dos conjuntos de registros
        /// </summary>
        private IEnumerable<Record> InnerJoin(
            List<Record> leftRecords, 
            List<Record> rightRecords, 
            List<string> joinFields)
        {
            var results = new List<Record>();
            
            // Agrupar registros del lado derecho por valores de clave
            var rightGroups = rightRecords
                .GroupBy(r => CreateJoinKey(r, joinFields))
                .ToDictionary(g => g.Key, g => g.ToList());
                
            // Para cada registro del lado izquierdo
            foreach (var leftRecord in leftRecords)
            {
                var leftKey = CreateJoinKey(leftRecord, joinFields);
                
                // Si hay coincidencias en el lado derecho
                if (rightGroups.TryGetValue(leftKey, out var matchingRightRecords))
                {
                    // Combinar con cada registro coincidente
                    foreach (var rightRecord in matchingRightRecords)
                    {
                        results.Add(MergeRecordPair(leftRecord, rightRecord));
                    }
                }
            }
            
            return results;
        }

        /// <summary>
        /// Realiza un LEFT JOIN entre dos conjuntos de registros
        /// </summary>
        private IEnumerable<Record> LeftJoin(
            List<Record> leftRecords, 
            List<Record> rightRecords, 
            List<string> joinFields)
        {
            var results = new List<Record>();
            
            // Agrupar registros del lado derecho por valores de clave
            var rightGroups = rightRecords
                .GroupBy(r => CreateJoinKey(r, joinFields))
                .ToDictionary(g => g.Key, g => g.ToList());
                
            // Para cada registro del lado izquierdo
            foreach (var leftRecord in leftRecords)
            {
                var leftKey = CreateJoinKey(leftRecord, joinFields);
                
                // Si hay coincidencias en el lado derecho
                if (rightGroups.TryGetValue(leftKey, out var matchingRightRecords))
                {
                    // Combinar con cada registro coincidente
                    foreach (var rightRecord in matchingRightRecords)
                    {
                        results.Add(MergeRecordPair(leftRecord, rightRecord));
                    }
                }
                else
                {
                    // No hay coincidencias, incluir solo el registro izquierdo
                    results.Add(leftRecord);
                }
            }
            
            return results;
        }

        /// <summary>
        /// Realiza un RIGHT JOIN entre dos conjuntos de registros
        /// </summary>
        private IEnumerable<Record> RightJoin(
            List<Record> leftRecords, 
            List<Record> rightRecords, 
            List<string> joinFields)
        {
            // Un RIGHT JOIN es equivalente a un LEFT JOIN invirtiendo los conjuntos
            return LeftJoin(rightRecords, leftRecords, joinFields);
        }

        /// <summary>
        /// Realiza un FULL OUTER JOIN entre dos conjuntos de registros
        /// </summary>
        private IEnumerable<Record> FullJoin(
            List<Record> leftRecords, 
            List<Record> rightRecords, 
            List<string> joinFields)
        {
            var results = new List<Record>();
            
            // Agrupar registros de ambos lados por valores de clave
            var leftGroups = leftRecords
                .GroupBy(r => CreateJoinKey(r, joinFields))
                .ToDictionary(g => g.Key, g => g.ToList());
                
            var rightGroups = rightRecords
                .GroupBy(r => CreateJoinKey(r, joinFields))
                .ToDictionary(g => g.Key, g => g.ToList());
                
            // Conjunto de claves ya procesadas
            var processedKeys = new HashSet<string>();
            
            // Procesar registros del lado izquierdo
            foreach (var leftRecord in leftRecords)
            {
                var leftKey = CreateJoinKey(leftRecord, joinFields);
                processedKeys.Add(leftKey);
                
                // Si hay coincidencias en el lado derecho
                if (rightGroups.TryGetValue(leftKey, out var matchingRightRecords))
                {
                    // Combinar con cada registro coincidente
                    foreach (var rightRecord in matchingRightRecords)
                    {
                        results.Add(MergeRecordPair(leftRecord, rightRecord));
                    }
                }
                else
                {
                    // No hay coincidencias, incluir solo el registro izquierdo
                    results.Add(leftRecord);
                }
            }
            
            // Procesar registros del lado derecho que no se han procesado aún
            foreach (var rightRecord in rightRecords)
            {
                var rightKey = CreateJoinKey(rightRecord, joinFields);
                
                // Si esta clave ya se procesó, omitir
                if (processedKeys.Contains(rightKey))
                    continue;
                    
                // No hay coincidencias en el lado izquierdo, incluir solo el registro derecho
                results.Add(rightRecord);
            }
            
            return results;
        }

        /// <summary>
        /// Crea una clave de unión basada en los valores de los campos especificados
        /// </summary>
        private string CreateJoinKey(Record record, List<string> joinFields)
        {
            return string.Join("||", joinFields.Select(field => 
                record.HasProperty(field) ? record.GetProperty(field)?.ToString() ?? "null" : "undefined"));
        }

        /// <summary>
        /// Combina un par de registros en uno solo
        /// </summary>
        private Record MergeRecordPair(Record leftRecord, Record rightRecord)
        {
            var result = new Record();
            
            // Copiar propiedades del registro izquierdo
            foreach (var property in leftRecord.GetProperties())
            {
                result.SetProperty(property.Key, property.Value);
            }
            
            // Copiar propiedades del registro derecho (con prefijo para evitar colisiones)
            foreach (var property in rightRecord.GetProperties())
            {
                // Si la propiedad ya existe, usar un prefijo
                string key = result.HasProperty(property.Key) 
                    ? $"Right_{property.Key}" 
                    : property.Key;
                    
                result.SetProperty(key, property.Value);
            }
            
            // Fusionar metadatos
            foreach (var meta in leftRecord.Metadata)
            {
                result.Metadata[meta.Key] = meta.Value;
            }
            
            foreach (var meta in rightRecord.Metadata)
            {
                if (!result.Metadata.ContainsKey(meta.Key))
                {
                    result.Metadata[meta.Key] = meta.Value;
                }
            }
            
            // Agregar metadatos sobre la combinación
            result.Metadata["MergeApplied"] = true;
            
            return result;
        }
    }
} 