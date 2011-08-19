using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.Reactive;
using BTDB.IL;

namespace BTDB.ServiceLayer
{
    public class Service : IService, IServiceInternalClient
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

        readonly object _serverServiceLock = new object();
        ulong _lastServerServiceId;
        readonly Dictionary<object, ulong> _serverServices = new Dictionary<object, ulong>();
        readonly NumberAllocator _serverTypeNumbers = new NumberAllocator(0);
        readonly Dictionary<uint, TypeInf> _serverTypeInfs = new Dictionary<uint, TypeInf>();

        readonly Dictionary<uint, TypeInf> _clientTypeInfs = new Dictionary<uint, TypeInf>();
        readonly Dictionary<uint, uint> _clientKnownServicesTypes = new Dictionary<uint, uint>();
        readonly Dictionary<uint, BindInf> _clientBindings = new Dictionary<uint, BindInf>();
        readonly NumberAllocator _clientBindNumbers = new NumberAllocator((uint)Command.FirstToBind);
        readonly NumberAllocator _clientAckNumbers = new NumberAllocator(0);
        readonly ConcurrentDictionary<uint, TaskAndBindInf> _clientAcks = new ConcurrentDictionary<uint, TaskAndBindInf>();

        struct TaskAndBindInf
        {
            public object TaskCompletionSource;
            public BindInf Binding;

            public TaskAndBindInf(BindInf binding, object taskCompletionSource)
            {
                Binding = binding;
                TaskCompletionSource = taskCompletionSource;
            }
        }

        public Service(IChannel channel)
        {
            _channel = channel;
            _lastServerServiceId = 0;
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
                    // TODO
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
                    _clientTypeInfs.Add(typeId, new TypeInf(reader));
                    break;
                case Subcommand.RegisterService:
                    var serviceId = reader.ReadVUInt32();
                    _clientKnownServicesTypes.Add(serviceId, reader.ReadVUInt32());
                    break;
                case Subcommand.UnregisterService:
                    OnUnregisterService(reader.ReadVUInt32());
                    break;
                case Subcommand.Bind:
                    // TODO
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        void OnUnregisterService(uint serviceId)
        {
            _clientKnownServicesTypes.Remove(serviceId);
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
            lock (_serverServiceLock)
            {
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
                var bindings = new List<BindInf>();
                var bindingFields = new List<FieldBuilder>();
                ILGenerator ilGenerator;
                foreach (var methodInfo in serviceType.GetMethods())
                {
                    var bindingField = tb.DefineField(string.Format("_b{0}", bindings.Count.ToString()), typeof(BindInf), FieldAttributes.Private);
                    bindingFields.Add(bindingField);
                    var parameterTypes = methodInfo.GetParameters().Select(pi => pi.ParameterType).ToArray();
                    var methodBuilder = tb.DefineMethod(methodInfo.Name, MethodAttributes.Public | MethodAttributes.Virtual, methodInfo.ReturnType, parameterTypes);
                    ilGenerator = methodBuilder.GetILGenerator(symbolDocumentWriter, 16);
                    var targetMethodInf = bestServiceTypeInf.MethodInfs.First(minf => minf.Name == methodInfo.Name);
                    var targetMethodIndex = Array.IndexOf(bestServiceTypeInf.MethodInfs, targetMethodInf);
                    var bindingId = _clientBindNumbers.Allocate();
                    var bindingInf = new BindInf
                        {
                            BindingId = bindingId,
                            ServiceId = bestServiceId,
                            MethodId = (uint)targetMethodIndex,
                            OneWay = false,
                            HandleResult = (t, reader) => ((TaskCompletionSource<int>)t).TrySetResult(reader.ReadInt32()),
                            HandleException = (t, ex) => ((TaskCompletionSource<int>)t).TrySetException(ex),
                            TaskWithSourceCreator = () =>
                                {
                                    var source = new TaskCompletionSource<int>();
                                    return new TaskWithSource(source, source.Task);
                                }
                        };
                    _clientBindings.Add(bindingId, bindingInf);
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
                        parameterInf.FieldHandler.SaveFromWillLoad(ilGenerator, il => il.Ldloc(writerLocal), il => il.Ldarg(order));
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
                var constructorParams = new[] { typeof(IServiceInternalClient), typeof(BindInf[]) };
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
                return finalType.GetConstructor(constructorParams).Invoke(new object[] { this, bindings.ToArray() });
            }
        }

        int EvaluateCompatibility(TypeInf from, TypeInf to)
        {
            return 1;
        }

        public void RegisterMyService(object service)
        {
            if (service == null) throw new ArgumentNullException("service");
            lock (_serverServiceLock)
            {
                var serviceId = ++_lastServerServiceId;
                _serverServices.Add(service, serviceId);
                Type type = service.GetType();
                var typeId = _serverTypeNumbers.Allocate();
                var typeInf = new TypeInf(type);
                _serverTypeInfs.Add(typeId, typeInf);
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
                writer.WriteVUInt32((uint)serviceId);
                writer.WriteVUInt32(typeId);
                writer.Dispose();
                _channel.Send(ByteBuffer.NewAsync(writer.Data));
            }
        }

        public void UnregisterMyService(object service)
        {
            lock (_serverServiceLock)
            {
                ulong serviceId;
                if (_serverServices.TryGetValue(service, out serviceId))
                {
                    _serverServices.Remove(service);
                    var writer = new ByteArrayWriter();
                    writer.WriteVUInt32((uint)Command.Subcommand);
                    writer.WriteVUInt32((uint)Subcommand.UnregisterService);
                    writer.WriteVUInt32((uint)serviceId);
                    writer.Dispose();
                    _channel.Send(ByteBuffer.NewAsync(writer.Data));
                }
            }
        }

        public IChannel Channel
        {
            get { return _channel; }
        }

        public AbstractBufferedWriter StartTwoWayMarshaling(BindInf binding, out Task resultReturned)
        {
            var message = new ByteArrayWriter();
            message.WriteVUInt32(binding.BindingId);
            var taskWithSource = binding.TaskWithSourceCreator();
            resultReturned = taskWithSource.Task;
            var ackId = _clientAckNumbers.Allocate();
            _clientAcks.TryAdd(ackId, new TaskAndBindInf(binding, taskWithSource.Source));
            return message;
        }

        public void FinishTwoWayMarshaling(AbstractBufferedWriter writer)
        {
            ((ByteArrayWriter)writer).Dispose();
            _channel.Send(ByteBuffer.NewAsync(((ByteArrayWriter)writer).Data));
        }
    }

    public interface IServiceInternalClient
    {
        AbstractBufferedWriter StartTwoWayMarshaling(BindInf binding, out Task resultReturned);
        void FinishTwoWayMarshaling(AbstractBufferedWriter writer);
    }

    public class BindInf
    {
        internal uint BindingId { get; set; }
        internal uint ServiceId { get; set; }
        internal uint MethodId { get; set; }
        internal bool OneWay { get; set; }
        internal Action<object, AbstractBufferedReader> HandleResult { get; set; }
        internal Action<object, Exception> HandleException { get; set; }
        internal Func<TaskWithSource> TaskWithSourceCreator { get; set; }

        internal BindInf() { }

        internal BindInf(AbstractBufferedReader reader)
        {
            ServiceId = reader.ReadVUInt32();
            MethodId = reader.ReadVUInt32();
            OneWay = reader.ReadBool();
        }

        internal void Store(AbstractBufferedWriter writer)
        {
            writer.WriteVUInt32(ServiceId);
            writer.WriteVUInt32(MethodId);
            writer.WriteBool(OneWay);
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
