using System.Reflection;
using System.Runtime.Loader;

namespace Kafka.Connect.Utilities
{
    public class PluginLoadContext : AssemblyLoadContext
    {
        private readonly bool _loadToDefault;
        private readonly AssemblyDependencyResolver _resolver;

        public PluginLoadContext(string pluginPath, bool loadToDefault = false)
        {
            _loadToDefault = loadToDefault;
            _resolver = new AssemblyDependencyResolver(pluginPath);
        }

        protected override Assembly Load(AssemblyName assemblyName)
        {
            if (_loadToDefault) return null;
            var assemblyPath = _resolver.ResolveAssemblyToPath(assemblyName);
            return assemblyPath != null ? LoadFromAssemblyPath(assemblyPath) : null;
        }
    }
}