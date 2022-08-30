using System;
using System.Reflection;
using System.Runtime.ExceptionServices;
using Kafka.Connect.Plugin.Logging;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Logging
{
    public class LogDispatchProxy<T> : DispatchProxy
    {
        private T _decorated;
        private ILogger _logger;

        public static T Create(T decorated, ILogger logger)
        {
            object proxy = Create<T, LogDispatchProxy<T>>() ;
            (proxy as LogDispatchProxy<T>)?.SetParameters(decorated, logger);
            return (T)proxy;
        }

        public string GetTypeName()
        {
            return _decorated.GetType().FullName;
        }

        protected void SetParameters(T decorated, ILogger logger)
        {
            if (decorated == null)
            {
                throw new ArgumentNullException(nameof(decorated));
            }

            _decorated = decorated;
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        protected override object Invoke(MethodInfo targetMethod, object[] args)
        {
            try
            {
                var operationLog = targetMethod
                    .GetImplementationMethodInfo(_decorated)
                    .GetCustomAttribute<OperationLogAttribute>();

                return operationLog == null
                    ? targetMethod.Invoke(_decorated, args)
                    : targetMethod.Invoke(_decorated, args, _logger, operationLog.Message, operationLog.Data);
            }
            catch (TargetInvocationException ex)
            {
                if (ex.InnerException != null)
                {
                    ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                }

                throw;
            }
        }
    }
}