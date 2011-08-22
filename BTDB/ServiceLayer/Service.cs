using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer;
using BTDB.Reactive;
using BTDB.IL;

namespace BTDB.ServiceLayer
{
    public class Service : IService, IServiceInternalClient, IServiceInternalServer
    {
        enum Command : uint
        {
            Subcommand = 0,
            Result = 1,
            Exception = 2,
            FirstToBind = 3
        }

        enum Subcommand : uint
        {
            RegisterType = 0,
            RegisterService = 1,
            UnregisterService = 2,
            Bind = 3,
        }

        readonly IChannel _channel;

        readonly ConcurrentDictionary<object, uint> _serverServices = new ConcurrentDictionary<object, uint>();
        readonly NumberAllocator _serverObjectNumbers = new NumberAllocator(0);
        readonly ConcurrentDictionary<uint, object> _serverObjects = new ConcurrentDictionary<uint, object>();
        readonly NumberAllocator _serverTypeNumbers = new NumberAllocator(0);
        readonly ConcurrentDictionary<uint, uint> _serverKnownServicesTypes = new ConcurrentDictionary<uint, uint>();
        readonly ConcurrentDictionary<uint, TypeInf> _serverTypeInfs = new ConcurrentDictionary<uint, TypeInf>();
        readonly ConcurrentDictionary<uint, ServerBindInf> _serverBindings = new ConcurrentDictionary<uint, ServerBindInf>();

        readonly ConcurrentDictionary<uint, TypeInf> _clientTypeInfs = new ConcurrentDictionary<uint, TypeInf>();
        readonly ConcurrentDictionary<uint, uint> _clientKnownServicesTypes = new ConcurrentDictionary<uint, uint>();
        readonly ConcurrentDictionary<uint, ClientBindInf> _clientBindings = new ConcurrentDictionary<uint, ClientBindInf>();
        readonly NumberAllocator _clientBindNumbers = new NumberAllocator((uint)Command.FirstToBind);
        readonly NumberAllocator _clientAckNumbers = new NumberAllocator(0);
        readonly ConcurrentDictionary<uint, TaskAndBindInf> _clientAcks = new ConcurrentDictionary<uint, TaskAndBindInf>();

        readonly DefaultTypeConvertorGenerator _typeConvertorGenerator = new DefaultTypeConvertorGenerator();

        struct TaskAndBindInf
        {
            public readonly object TaskCompletionSource;
            public readonly ClientBindInf Binding;

            public TaskAndBindInf(ClientBindInf binding, object taskCompletionSource)
            {
                Binding = binding;
                TaskCompletionSource = taskCompletionSource;
            }
        }

        public Service(IChannel channel)
        {
            _channel = channel;
            channel.OnReceive.FastSubscribe(OnReceive);
        }

        void OnReceive(ByteBuffer obj)
        {
            var reader = new ByteBufferReader(obj);
            var c0 = reader.ReadVUInt32();
            uint ackId;
            TaskAndBindInf taskAndBind;
            switch ((Command)c0)
            {
                case Command.Subcommand:
                    OnSubcommand(reader);
                    break;
                case Command.Result:
                    ackId = reader.ReadVUInt32();
                    if (_clientAcks.TryRemove(ackId, out taskAndBind))
                    {
                        _clientAckNumbers.Deallocate(ackId);
                        taskAndBind.Binding.HandleResult(taskAndBind.TaskCompletionSource, reader);
                    }
                    break;
                case Command.Exception:
                    ackId = reader.ReadVUInt32();
                    if (_clientAcks.TryRemove(ackId, out taskAndBind))
                    {
                        _clientAckNumbers.Deallocate(ackId);
                        var ex = new Exception(reader.ReadString());
                        taskAndBind.Binding.HandleException(taskAndBind.TaskCompletionSource, ex);
                    }
                    break;
                default:
                    ServerBindInf serverBindInf;
                    if (_serverBindings.TryGetValue(c0, out serverBindInf))
                        serverBindInf.Runner(serverBindInf.Object, reader, this);
                    else
                        throw new InvalidDataException();
                    break;
            }
        }

        void OnSubcommand(ByteBufferReader reader)
        {
            var c1 = reader.ReadVUInt32();
            switch ((Subcommand)c1)
            {
                case Subcommand.RegisterType:
                    var typeId = reader.ReadVUInt32();
                    _clientTypeInfs.TryAdd(typeId, new TypeInf(reader));
                    break;
                case Subcommand.RegisterService:
                    var serviceId = reader.ReadVUInt32();
                    _clientKnownServicesTypes.TryAdd(serviceId, reader.ReadVUInt32());
                    break;
                case Subcommand.UnregisterService:
                    OnUnregisterService(reader.ReadVUInt32());
                    break;
                case Subcommand.Bind:
                    OnBind(reader);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        void OnBind(AbstractBufferedReader reader)
        {
            var binding = new ServerBindInf(reader);
            object serverObject;
            _serverObjects.TryGetValue(binding.ServiceId, out serverObject);
            uint typeId;
            _serverKnownServicesTypes.TryGetValue(binding.ServiceId, out typeId);
            TypeInf typeInf;
            _serverTypeInfs.TryGetValue(typeId, out typeInf);
            var methodInf = typeInf.MethodInfs[binding.MethodId];
            binding.Object = serverObject;
            var method = new DynamicMethod(string.Format("{0}_{1}", typeInf.Name, methodInf.Name), typeof(void), new[] { typeof(object), typeof(AbstractBufferedReader), typeof(IServiceInternalServer) });
            var ilGenerator = method.GetILGenerator();
            var localResultId = ilGenerator.DeclareLocal(typeof(uint));
            var localWriter = ilGenerator.DeclareLocal(typeof(AbstractBufferedWriter));
            var localException = ilGenerator.DeclareLocal(typeof(Exception));
            var localParams = new LocalBuilder[methodInf.Parameters.Length];
            var localResult = ilGenerator.DeclareLocal(methodInf.ResultFieldHandler.WillLoad());
            ilGenerator
                .Ldarg(1)
                .Callvirt(() => ((AbstractBufferedReader)null).ReadVUInt32())
                .Stloc(localResultId)
                .BeginExceptionBlock();
            for (int i = 0; i < methodInf.Parameters.Length; i++)
            {
                var fieldHandler = methodInf.Parameters[i].FieldHandler;
                localParams[i] = ilGenerator.DeclareLocal(methodInf.MethodInfo.GetParameters()[i].ParameterType);
                fieldHandler.LoadToWillLoad(ilGenerator, il => il.Ldarg(1));
                _typeConvertorGenerator.GenerateConversion(fieldHandler.WillLoad(), localParams[i].LocalType)
                    (ilGenerator);
                ilGenerator.Stloc(localParams[i]);
            }
            ilGenerator
                .Ldarg(0)
                .Castclass(serverObject.GetType());
            for (int i = 0; i < methodInf.Parameters.Length; i++)
            {
                ilGenerator.Ldloc(localParams[i]);
            }
            ilGenerator
                .Callvirt(methodInf.MethodInfo);
            _typeConvertorGenerator.GenerateConversion(methodInf.MethodInfo.ReturnType, localResult.LocalType)(ilGenerator);
            ilGenerator
                .Stloc(localResult)
                .Ldarg(2)
                .Ldloc(localResultId)
                .Callvirt(() => ((IServiceInternalServer)null).StartResultMarshaling(0u))
                .Stloc(localWriter);
            methodInf.ResultFieldHandler.SaveFromWillLoad(ilGenerator, il => il.Ldloc(localWriter), il => il.Ldloc(localResult));
            ilGenerator
                .Ldarg(2)
                .Ldloc(localWriter)
                .Callvirt(() => ((IServiceInternalServer)null).FinishResultMarshaling(null));
            ilGenerator
                .Catch(typeof(Exception))
                .Stloc(localException)
                .Ldarg(2)
                .Ldloc(localResultId)
                .Ldloc(localException)
                .Callvirt(() => ((IServiceInternalServer)null).ExceptionMarshaling(0u, null))
                .EndExceptionBlock();
            ilGenerator.Ret();

            binding.Runner = method.CreateDelegate<Action<object, AbstractBufferedReader, IServiceInternalServer>>();
            _serverBindings.TryAdd(binding.BindingId, binding);
        }

        void OnUnregisterService(uint serviceId)
        {
            uint placebo;
            _clientKnownServicesTypes.TryRemove(serviceId, out placebo);
        }

        public void Dispose()
        {
            _channel.Dispose();
        }

        public T QueryOtherService<T>() where T : class
        {
            return (T)QueryOtherService(typeof(T));
        }

        public object QueryOtherService(Type serviceType)
        {
            if (serviceType == null) throw new ArgumentNullException("serviceType");
            var typeInf = new TypeInf(serviceType);
            var bestMatch = int.MinValue;
            var bestServiceId = 0u;
            TypeInf bestServiceTypeInf = null;
            foreach (var servicesType in _clientKnownServicesTypes)
            {
                var targetTypeInf = _clientTypeInfs[servicesType.Value];
                var score = EvaluateCompatibility(typeInf, targetTypeInf);
                if (score > bestMatch)
                {
                    bestMatch = score;
                    bestServiceId = servicesType.Key;
                    bestServiceTypeInf = targetTypeInf;
                }
            }
            if (bestMatch <= 0) return null;
            var name = serviceType.Name;
            var ab = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name + "Asm"), AssemblyBuilderAccess.RunAndCollect);
            var mb = ab.DefineDynamicModule(name + "Asm.dll", true);
            var symbolDocumentWriter = mb.DefineDocument("just_dynamic_" + name, Guid.Empty, Guid.Empty, Guid.Empty);
            var tb = mb.DefineType(name + "Impl", TypeAttributes.Public, typeof(object), new[] { serviceType });
            var ownerField = tb.DefineField("_owner", typeof(IServiceInternalClient), FieldAttributes.Private);
            var bindings = new List<ClientBindInf>();
            var bindingFields = new List<FieldBuilder>();
            ILGenerator ilGenerator;
            foreach (var methodInfo in serviceType.GetMethods())
            {
                var bindingField = tb.DefineField(string.Format("_b{0}", bindings.Count.ToString()), typeof(ClientBindInf), FieldAttributes.Private);
                bindingFields.Add(bindingField);
                var parameterTypes = methodInfo.GetParameters().Select(pi => pi.ParameterType).ToArray();
                var methodBuilder = tb.DefineMethod(methodInfo.Name, MethodAttributes.Public | MethodAttributes.Virtual, methodInfo.ReturnType, parameterTypes);
                ilGenerator = methodBuilder.GetILGenerator(symbolDocumentWriter, 16);
                var targetMethodInf = bestServiceTypeInf.MethodInfs.First(minf => minf.Name == methodInfo.Name);
                var targetMethodIndex = Array.IndexOf(bestServiceTypeInf.MethodInfs, targetMethodInf);
                var bindingId = _clientBindNumbers.Allocate();
                var bindingInf = new ClientBindInf
                    {
                        BindingId = bindingId,
                        ServiceId = bestServiceId,
                        MethodId = (uint)targetMethodIndex,
                        OneWay = false,
                        HandleResult = (t, reader) => ((TaskCompletionSource<int>)t).TrySetResult((int)reader.ReadVInt64()),
                        HandleException = (t, ex) => ((TaskCompletionSource<int>)t).TrySetException(ex),
                        TaskWithSourceCreator = () =>
                            {
                                var source = new TaskCompletionSource<int>();
                                return new TaskWithSource(source, source.Task);
                            }
                    };
                _clientBindings.TryAdd(bindingId, bindingInf);
                bindings.Add(bindingInf);
                var writer = new ByteArrayWriter();
                writer.WriteVUInt32((uint)Command.Subcommand);
                writer.WriteVUInt32((uint)Subcommand.Bind);
                bindingInf.Store(writer);
                writer.Dispose();
                _channel.Send(ByteBuffer.NewAsync(writer.Data));
                var resultTaskLocal = ilGenerator.DeclareLocal(typeof(Task));
                var writerLocal = ilGenerator.DeclareLocal(typeof(AbstractBufferedWriter));
                Task placebo;
                ilGenerator
                    .Ldarg(0)
                    .Ldfld(ownerField)
                    .Ldarg(0)
                    .Ldfld(bindingField)
                    .Ldloca(resultTaskLocal)
                    .Callvirt(() => ((IServiceInternalClient)null).StartTwoWayMarshaling(null, out placebo))
                    .Stloc(writerLocal);
                uint paramOrder = 0;
                foreach (var parameterInf in targetMethodInf.Parameters)
                {
                    var order = (ushort)(1 + paramOrder);
                    var convGen = _typeConvertorGenerator.GenerateConversion(parameterTypes[paramOrder], parameterInf.FieldHandler.WillLoad());
                    parameterInf.FieldHandler.SaveFromWillLoad(ilGenerator, il => il.Ldloc(writerLocal), il =>
                        {
                            il.Ldarg(order);
                            convGen(il);
                        });
                    paramOrder++;
                }
                ilGenerator
                    .Ldarg(0)
                    .Ldfld(ownerField)
                    .Ldloc(writerLocal)
                    .Callvirt(() => ((IServiceInternalClient)null).FinishTwoWayMarshaling(null))
                    .Ldloc(resultTaskLocal)
                    .Castclass(typeof(Task<int>))
                    .Callvirt(typeof(Task<int>).GetMethod("get_Result"))
                    .Ret();
                tb.DefineMethodOverride(methodBuilder, methodInfo);
            }
            var constructorParams = new[] { typeof(IServiceInternalClient), typeof(ClientBindInf[]) };
            var contructorBuilder = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard,
                constructorParams);
            ilGenerator = contructorBuilder.GetILGenerator();
            ilGenerator
                .Ldarg(0)
                .Call(() => new object())
                .Ldarg(0)
                .Ldarg(1)
                .Stfld(ownerField);
            for (int i = 0; i < bindingFields.Count; i++)
            {
                ilGenerator
                    .Ldarg(0)
                    .Ldarg(2)
                    .LdcI4(i)
                    .LdelemRef()
                    .Stfld(bindingFields[i]);
            }
            ilGenerator.Ret();
            var finalType = tb.CreateType();
            //ab.Save(mb.ScopeName);
            return finalType.GetConstructor(constructorParams).Invoke(new object[] { this, bindings.ToArray() });
        }

        int EvaluateCompatibility(TypeInf from, TypeInf to)
        {
            return 1;
        }

        public void RegisterMyService(object service)
        {
            if (service == null) throw new ArgumentNullException("service");
            var serviceId = _serverObjectNumbers.Allocate();
            _serverObjects.TryAdd(serviceId, service);
            _serverServices.TryAdd(service, serviceId);
            Type type = service.GetType();
            var typeId = _serverTypeNumbers.Allocate();
            var typeInf = new TypeInf(type);
            _serverTypeInfs.TryAdd(typeId, typeInf);
            var writer = new ByteArrayWriter();
            writer.WriteVUInt32((uint)Command.Subcommand);
            writer.WriteVUInt32((uint)Subcommand.RegisterType);
            writer.WriteVUInt32(typeId);
            typeInf.Store(writer);
            writer.Dispose();
            _channel.Send(ByteBuffer.NewAsync(writer.Data));
            writer = new ByteArrayWriter();
            writer.WriteVUInt32((uint)Command.Subcommand);
            writer.WriteVUInt32((uint)Subcommand.RegisterService);
            writer.WriteVUInt32(serviceId);
            writer.WriteVUInt32(typeId);
            writer.Dispose();
            _channel.Send(ByteBuffer.NewAsync(writer.Data));
        }

        public void UnregisterMyService(object service)
        {
            uint serviceId;
            if (_serverServices.TryRemove(service, out serviceId))
            {
                var writer = new ByteArrayWriter();
                writer.WriteVUInt32((uint)Command.Subcommand);
                writer.WriteVUInt32((uint)Subcommand.UnregisterService);
                writer.WriteVUInt32(serviceId);
                writer.Dispose();
                _channel.Send(ByteBuffer.NewAsync(writer.Data));
            }
        }

        public IChannel Channel
        {
            get { return _channel; }
        }

        public AbstractBufferedWriter StartTwoWayMarshaling(ClientBindInf binding, out Task resultReturned)
        {
            var message = new ByteArrayWriter();
            message.WriteVUInt32(binding.BindingId);
            var taskWithSource = binding.TaskWithSourceCreator();
            resultReturned = taskWithSource.Task;
            var ackId = _clientAckNumbers.Allocate();
            _clientAcks.TryAdd(ackId, new TaskAndBindInf(binding, taskWithSource.Source));
            message.WriteVUInt32(ackId);
            return message;
        }

        public void FinishTwoWayMarshaling(AbstractBufferedWriter writer)
        {
            ((ByteArrayWriter)writer).Dispose();
            _channel.Send(ByteBuffer.NewAsync(((ByteArrayWriter)writer).Data));
        }

        public AbstractBufferedWriter StartResultMarshaling(uint resultId)
        {
            var message = new ByteArrayWriter();
            message.WriteVUInt32((uint)Command.Result);
            message.WriteVUInt32(resultId);
            return message;
        }

        public void FinishResultMarshaling(AbstractBufferedWriter writer)
        {
            ((ByteArrayWriter)writer).Dispose();
            _channel.Send(ByteBuffer.NewAsync(((ByteArrayWriter)writer).Data));
        }

        public void ExceptionMarshaling(uint resultId, Exception ex)
        {
            var message = new ByteArrayWriter();
            message.WriteVUInt32((uint)Command.Exception);
            message.WriteVUInt32(resultId);
            message.WriteString(ex.Message);
            message.Dispose();
            _channel.Send(ByteBuffer.NewAsync(message.Data));
        }
    }

    public interface IServiceInternalClient
    {
        AbstractBufferedWriter StartTwoWayMarshaling(ClientBindInf binding, out Task resultReturned);
        void FinishTwoWayMarshaling(AbstractBufferedWriter writer);
    }

    public interface IServiceInternalServer
    {
        AbstractBufferedWriter StartResultMarshaling(uint resultId);
        void FinishResultMarshaling(AbstractBufferedWriter writer);
        void ExceptionMarshaling(uint resultId, Exception ex);
    }

    public class ClientBindInf
    {
        internal uint BindingId { get; set; }
        internal uint ServiceId { get; set; }
        internal uint MethodId { get; set; }
        internal bool OneWay { get; set; }
        internal Action<object, AbstractBufferedReader> HandleResult { get; set; }
        internal Action<object, Exception> HandleException { get; set; }
        internal Func<TaskWithSource> TaskWithSourceCreator { get; set; }

        internal ClientBindInf() { }

        internal void Store(AbstractBufferedWriter writer)
        {
            writer.WriteVUInt32(BindingId);
            writer.WriteVUInt32(ServiceId);
            writer.WriteVUInt32(MethodId);
            writer.WriteBool(OneWay);
        }
    }

    public class ServerBindInf
    {
        internal uint BindingId { get; set; }
        internal uint ServiceId { get; set; }
        internal uint MethodId { get; set; }
        internal bool OneWay { get; set; }
        internal object Object { get; set; }
        internal Action<object, AbstractBufferedReader, IServiceInternalServer> Runner { get; set; }

        internal ServerBindInf(AbstractBufferedReader reader)
        {
            BindingId = reader.ReadVUInt32();
            ServiceId = reader.ReadVUInt32();
            MethodId = reader.ReadVUInt32();
            OneWay = reader.ReadBool();
        }

    }

    internal struct TaskWithSource
    {
        internal object Source;
        internal Task Task;

        public TaskWithSource(object source, Task task)
        {
            Source = source;
            Task = task;
        }
    }
}
