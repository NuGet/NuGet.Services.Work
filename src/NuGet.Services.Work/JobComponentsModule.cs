using Autofac;
using NuGet.Services.Configuration;

namespace NuGet.Services.Work
{
    public class JobComponentsModule : Module
    {
        private InvocationQueue _queue;
        private string _instanceName;
        
        public JobComponentsModule(string instanceName) : this(instanceName, null) { }
        public JobComponentsModule(string instanceName, InvocationQueue queue)
        {
            _instanceName = instanceName;
            _queue = queue;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<JobRunner>()
                .AsSelf()
                .SingleInstance();
            builder.RegisterType<JobDispatcher>()
                .AsSelf()
                .SingleInstance();

            if (_queue != null)
            {
                builder.RegisterInstance(_queue).As<InvocationQueue>();
            }
            else
            {
                builder
                    .RegisterType<InvocationQueue>()
                    .AsSelf()
                    .UsingConstructor(
                        typeof(Clock),
                        typeof(string),
                        typeof(ConfigurationHub))
                    .WithParameter(
                        new NamedParameter("instanceName", _instanceName));
            }
        }
    }
}