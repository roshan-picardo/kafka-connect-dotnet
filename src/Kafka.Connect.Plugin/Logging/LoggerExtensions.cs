using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Plugin.Logging
{
    public static class LoggerExtensions
    {
        public static IServiceCollection AddScopedWithLogging<TService, TImplementation>(this IServiceCollection services)
            where TService : class
            where TImplementation : class, TService
        {
            return services
                .AddScoped<TImplementation>()
                .AddScoped(provider =>
                    provider.GetService<ILogDecorator>()?.Build<TService>(
                        provider.GetService<TImplementation>(),
                        provider.GetService<ILoggerFactory>()?.CreateLogger<TImplementation>()));
        }
    }
}