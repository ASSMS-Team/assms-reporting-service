using System.Reflection;
using System.Net;

using ReportingService.Messaging.Consumers;
using ReportingService.Repositories;
using Microsoft.AspNetCore.HttpOverrides;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("default")
    ?? throw new InvalidOperationException("Connection string 'default' was not found.");

// The address of something this service talks to but does not own, so it may
// not be hard-coded - staging and production point elsewhere. Thrown on rather
// than defaulted: a consumer silently pointed at localhost would start, log
// nothing alarming, and project nothing.
var kafkaBootstrapServers = builder.Configuration["Kafka:BootstrapServers"]
    ?? throw new InvalidOperationException("Configuration value 'Kafka:BootstrapServers' was not found.");

// Add services to the container.

// Empty rather than a fallback origin when the key is absent: a missing
// configuration should refuse every browser origin, not quietly allow one.
var allowedOrigins = builder.Configuration.GetSection("Cors:AllowedOrigins").Get<string[]>()
    ?? Array.Empty<string>();

builder.Services.AddCors(options =>
{
    options.AddPolicy(FrontendCorsPolicy, policy => policy
        .WithOrigins(allowedOrigins)
        .AllowAnyHeader()
        .AllowAnyMethod());
});

// Nginx reaches the loopback-published container through Docker's bridge
// gateway. Trust only that proxy address when consuming client and scheme
// headers; requests from arbitrary networks cannot supply forwarded headers.
builder.Services.Configure<ForwardedHeadersOptions>(options =>
{
    options.ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
    options.KnownNetworks.Clear();
    options.KnownProxies.Clear();
    options.KnownProxies.Add(IPAddress.Parse("172.17.0.1").MapToIPv6());
});

builder.Services.AddSingleton<IDbConnectionFactory>(new MySqlConnectionFactory(connectionString));
builder.Services.AddScoped<IJobProjectionRepository, JobProjectionRepository>();

// A hosted service is a singleton, which is why it takes the scope factory and
// not the repository - see the note in JobCreatedConsumer.
builder.Services.AddHostedService(serviceProvider =>
    new JobCreatedConsumer(
        kafkaBootstrapServers,
        serviceProvider.GetRequiredService<IServiceScopeFactory>(),
        serviceProvider.GetRequiredService<ILogger<JobCreatedConsumer>>()));

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    // Built from the assembly name so a project rename does not silently drop
    // the descriptions; the file sits next to the DLL in the output folder.
    var xmlFilename = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
    options.IncludeXmlComments(Path.Combine(AppContext.BaseDirectory, xmlFilename));
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseForwardedHeaders();

app.UseHttpsRedirection();

app.UseCors(FrontendCorsPolicy);

app.UseAuthorization();

app.MapControllers();

app.Run();

partial class Program
{
    private const string FrontendCorsPolicy = "Frontend";
}
