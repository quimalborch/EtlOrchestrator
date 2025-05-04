using EtlOrchestrator.Infrastructure;
using Microsoft.OpenApi.Models;
using System.Reflection;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "ETL Orchestrator API",
        Version = "v1",
        Description = "API for ETL workflow management",
        Contact = new OpenApiContact
        {
            Name = "Developer",
            Email = "quimalborch@gmail.com"
        }
    });
});

// Agregar controladores MVC
builder.Services.AddControllers();

// Registrar servicios de infraestructura
builder.Services.AddInfrastructure(builder.Configuration);

// Registrar HttpClientFactory para HttpApiSourceConnector
builder.Services.AddHttpClient();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "ETL Orchestrator API v1");
        c.RoutePrefix = string.Empty;
    });
}

app.UseHttpsRedirection();

// Mapear controladores
app.MapControllers();

app.Run();

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
