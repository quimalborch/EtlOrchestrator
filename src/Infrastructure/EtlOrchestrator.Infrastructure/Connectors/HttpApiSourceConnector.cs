using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using EtlOrchestrator.Core;
using EtlOrchestrator.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace EtlOrchestrator.Infrastructure.Connectors
{
    /// <summary>
    /// Implementación de ISourceConnector para extraer datos desde una API HTTP
    /// </summary>
    public class HttpApiSourceConnector : ISourceConnector
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<HttpApiSourceConnector> _logger;

        public HttpApiSourceConnector(IHttpClientFactory httpClientFactory, ILogger<HttpApiSourceConnector> logger)
        {
            _httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Extrae datos de una API HTTP según la URL definida en el contexto
        /// </summary>
        /// <param name="context">Contexto con la configuración de la petición HTTP</param>
        /// <returns>Colección de registros extraídos</returns>
        public async Task<IEnumerable<Record>> ExtractAsync(Context context)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            // Obtener parámetros del contexto
            if (!context.TryGetParameter<string>("Url", out var url))
                throw new InvalidOperationException("El parámetro 'Url' es obligatorio");

            // Crear el cliente HTTP
            string clientName = context.GetParameter<string>("ClientName", "DefaultClient");
            var httpClient = _httpClientFactory.CreateClient(clientName);

            // Configurar timeout opcional
            if (context.TryGetParameter<int>("TimeoutSeconds", out var timeoutSeconds))
            {
                httpClient.Timeout = TimeSpan.FromSeconds(timeoutSeconds);
            }

            // Configurar headers opcionales
            var headers = context.GetParameter<Dictionary<string, string>>("Headers", null);
            if (headers != null)
            {
                foreach (var header in headers)
                {
                    httpClient.DefaultRequestHeaders.Add(header.Key, header.Value);
                }
            }

            // Configurar método HTTP (por defecto GET)
            var method = context.GetParameter<string>("Method", "GET");
            
            _logger.LogInformation("Iniciando extracción desde API HTTP. URL: {Url}, Método: {Method}", url, method);

            try
            {
                HttpResponseMessage response;
                if (method.Equals("POST", StringComparison.OrdinalIgnoreCase))
                {
                    var content = context.GetParameter<string>("Content", "");
                    var mediaType = context.GetParameter<string>("ContentType", "application/json");
                    response = await httpClient.PostAsync(url, new StringContent(content, System.Text.Encoding.UTF8, mediaType));
                }
                else
                {
                    response = await httpClient.GetAsync(url);
                }

                response.EnsureSuccessStatusCode();
                var jsonContent = await response.Content.ReadAsStringAsync();
                
                // Convertir la respuesta JSON a Records
                var records = ParseJsonToRecords(jsonContent, context);
                
                _logger.LogInformation("Extracción completada. Se obtuvieron {Count} registros", records.Count);
                
                return records;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al extraer datos de la API HTTP: {Message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Convierte un JSON en una colección de Records
        /// </summary>
        private List<Record> ParseJsonToRecords(string json, Context context)
        {
            var records = new List<Record>();
            var jsonPropertyPath = context.GetParameter<string>("JsonPropertyPath", null);
            
            try
            {
                using (JsonDocument document = JsonDocument.Parse(json))
                {
                    JsonElement root = document.RootElement;
                    
                    // Si se especifica una ruta de propiedad JSON, navegar hasta esa propiedad
                    if (!string.IsNullOrEmpty(jsonPropertyPath))
                    {
                        var pathParts = jsonPropertyPath.Split('.');
                        JsonElement current = root;
                        
                        foreach (var part in pathParts)
                        {
                            if (current.TryGetProperty(part, out var property))
                            {
                                current = property;
                            }
                            else
                            {
                                throw new InvalidOperationException($"La propiedad '{part}' no existe en el JSON de respuesta");
                            }
                        }
                        
                        root = current;
                    }
                    
                    // Si la raíz (o la propiedad especificada) es un array, procesar cada elemento como un Record
                    if (root.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var element in root.EnumerateArray())
                        {
                            records.Add(ConvertJsonElementToRecord(element));
                        }
                    }
                    // Si es un objeto único, convertirlo en un solo Record
                    else if (root.ValueKind == JsonValueKind.Object)
                    {
                        records.Add(ConvertJsonElementToRecord(root));
                    }
                    else
                    {
                        throw new InvalidOperationException("El JSON de respuesta no es un objeto ni un array");
                    }
                }
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Error al procesar el JSON: {Message}", ex.Message);
                throw;
            }
            
            return records;
        }

        /// <summary>
        /// Convierte un elemento JSON en un objeto Record
        /// </summary>
        private Record ConvertJsonElementToRecord(JsonElement element)
        {
            var record = new Record();
            
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in element.EnumerateObject())
                {
                    record.SetProperty(property.Name, GetJsonPropertyValue(property.Value));
                }
            }
            
            return record;
        }

        /// <summary>
        /// Obtiene el valor de una propiedad JSON convertido al tipo de dato apropiado
        /// </summary>
        private object GetJsonPropertyValue(JsonElement element)
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
                    if (element.TryGetDouble(out double doubleValue))
                        return doubleValue;
                    return element.GetRawText();
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.Null:
                    return null;
                case JsonValueKind.Object:
                case JsonValueKind.Array:
                    return element.GetRawText();
                default:
                    return element.GetRawText();
            }
        }
    }
} 