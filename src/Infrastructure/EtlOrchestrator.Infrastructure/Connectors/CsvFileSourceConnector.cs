using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Connectors
{
    /// <summary>
    /// Implementación de ISourceConnector para extraer datos desde archivos CSV
    /// </summary>
    public class CsvFileSourceConnector : ISourceConnector
    {
        private readonly ILogger<CsvFileSourceConnector> _logger;

        public CsvFileSourceConnector(ILogger<CsvFileSourceConnector> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Extrae datos de un archivo CSV
        /// </summary>
        /// <param name="context">Contexto con la configuración de la ruta del archivo y opciones CSV</param>
        /// <returns>Colección de registros extraídos</returns>
        public async Task<IEnumerable<Record>> ExtractAsync(Context context)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            // Obtener parámetros del contexto
            if (!context.TryGetParameter<string>("FilePath", out var filePath))
                throw new InvalidOperationException("El parámetro 'FilePath' es obligatorio");

            if (!File.Exists(filePath))
                throw new FileNotFoundException($"El archivo CSV no existe: {filePath}");

            // Configurar opciones de CsvHelper
            var delimiter = context.GetParameter<string>("Delimiter", ",");
            var hasHeaderRecord = context.GetParameter<bool>("HasHeaderRecord", true);
            var culture = context.GetParameter<string>("Culture", "en-US");
            var skipRows = context.GetParameter<int>("SkipRows", 0);
            var maxRows = context.GetParameter<int>("MaxRows", 0);

            var config = new CsvConfiguration(CultureInfo.GetCultureInfo(culture))
            {
                Delimiter = delimiter,
                HasHeaderRecord = hasHeaderRecord,
                IgnoreBlankLines = true,
                MissingFieldFound = null
            };

            _logger.LogInformation("Iniciando extracción desde archivo CSV: {FilePath}", filePath);

            try
            {
                var records = new List<Record>();
                
                // Utilizamos un StreamReader para poder procesar archivos grandes sin cargarlos completamente en memoria
                using (var reader = new StreamReader(filePath))
                using (var csv = new CsvReader(reader, config))
                {
                    // Saltar filas si es necesario
                    for (int i = 0; i < skipRows; i++)
                    {
                        await csv.ReadAsync();
                    }

                    // Leer los registros
                    var rowCount = 0;
                    await csv.ReadAsync();
                    
                    // Si tiene encabezado, leerlo primero
                    if (hasHeaderRecord)
                    {
                        csv.ReadHeader();
                    }

                    // Leer los registros
                    while (await csv.ReadAsync())
                    {
                        var record = new Record();
                        
                        // Si tiene encabezado, usar los nombres de las columnas como claves
                        if (hasHeaderRecord)
                        {
                            foreach (var header in csv.HeaderRecord)
                            {
                                record.SetProperty(header, csv.GetField(header));
                            }
                        }
                        // De lo contrario, usar índices como claves
                        else
                        {
                            for (int i = 0; i < csv.Parser.Count; i++)
                            {
                                record.SetProperty($"Column{i}", csv.GetField(i));
                            }
                        }
                        
                        records.Add(record);
                        rowCount++;
                        
                        // Si se especificó un máximo de filas y se alcanzó, detener la lectura
                        if (maxRows > 0 && rowCount >= maxRows)
                            break;
                    }
                    
                    _logger.LogInformation("Extracción completada. Se leyeron {Count} registros del archivo CSV", records.Count);
                }
                
                return records;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al extraer datos del archivo CSV: {Message}", ex.Message);
                throw;
            }
        }
    }
} 