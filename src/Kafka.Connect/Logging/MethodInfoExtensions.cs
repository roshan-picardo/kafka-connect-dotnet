using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Logging
{
    public static class MethodInfoExtensions
    {
         public static MethodInfo GetImplementationMethodInfo(this MethodInfo targetMethod, object obj)
         {
             if (targetMethod.DeclaringType == null) return null;
            var map = obj.GetType().GetInterfaceMap(targetMethod.DeclaringType);
            var index = Array.IndexOf(map.InterfaceMethods, targetMethod);

            return map.TargetMethods[index];
        }


         public static object Invoke(this MethodInfo methodInfo, object obj, object[] args, ILogger logger, string message, string[] data = null)
         {
             object result = null;
             if (!IsAsyncMethod(methodInfo))
             {
                 using var timed = new TimedLog(logger,  message, data);
                 result = methodInfo.Invoke(obj, args);
                 timed.Complete();
             }
             else
             {
                 var timed = new TimedLog(logger, message, data);

                 result = methodInfo.Invoke(obj, args);

                 ((Task)result)?.ContinueWith(task =>
                 {
                     try
                     {
                         if (!task.IsFaulted && !task.IsCanceled)
                         {
                             timed.Complete();
                         }
                     }
                     finally
                     {
                         timed.Dispose();
                     }
                 });
             }

             return result;
         }
         
        private static bool IsAsyncMethod(MethodInfo method)
        {
            return method.ReturnType.GetMethod(nameof(Task.GetAwaiter)) != null;
        }
    }
}