using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.Reactive;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class Service : IService, IServiceInternalClient, IServiceInternalServer, IServiceInternal
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
        readonly ConditionalWeakTable<Type, WeakReference> _otherServiceCache = new ConditionalWeakTable<Type, WeakReference>();

        ITypeConvertorGenerator _typeConvertorGenerator;
        IFieldHandlerFactory _fieldHandlerFactory;

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
            _fieldHandlerFactory = new DefaultFieldHandlerFactory(this);
            _channel = channel;
            _typeConvertorGenerator = new DefaultTypeConvertorGenerator();
            _fieldHandlerFactory = new DefaultServiceFieldHandlerFactory(this);
            channel.OnReceive.FastSubscribe(OnReceive, OnDisconnect);
        }

        void OnDisconnect()
        {
            foreach (var clientAck in _clientAcks)
            {
                clientAck.Value.Binding.HandleCancellation(clientAck.Value.TaskCompletionSource);
            }
            _clientAcks.Clear();
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
                        var ex = new BinaryFormatter().Deserialize(new MemoryStream(reader.ReadByteArray())) as Exception;
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
                    _clientTypeInfs.TryAdd(typeId, new TypeInf(reader, _fieldHandlerFactory));
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
            var returnType = methodInf.MethodInfo.ReturnType.UnwrapTask();
            var isAsync = returnType != methodInf.MethodInfo.ReturnType;
            binding.Object = serverObject;
            var method = new DynamicMethod<Action<object, AbstractBufferedReader, IServiceInternalServer>>(string.Format("{0}_{1}", typeInf.Name, methodInf.Name));
            var ilGenerator = method.GetILGenerator();
            LocalBuilder localResultId = null;
            var localWriter = ilGenerator.DeclareLocal(typeof(AbstractBufferedWriter));
            var localException = ilGenerator.DeclareLocal(typeof(Exception));
            var localParams = new LocalBuilder[methodInf.Parameters.Length];
            LocalBuilder localResult = null;
            if (!binding.OneWay)
            {
                localResultId = ilGenerator.DeclareLocal(typeof(uint));
                if (methodInf.ResultFieldHandler != null && !isAsync)
                    localResult = ilGenerator.DeclareLocal(methodInf.ResultFieldHandler.HandledType());
                ilGenerator
                    .Ldarg(1)
                    .Callvirt(() => ((AbstractBufferedReader)null).ReadVUInt32())
                    .Stloc(localResultId)
                    .BeginExceptionBlock();
            }
            var needsCtx = methodInf.Parameters.Any(p => p.FieldHandler.NeedsCtx());
            LocalBuilder localReaderCtx = null;
            if (needsCtx)
            {
                localReaderCtx = ilGenerator.DeclareLocal(typeof(IReaderCtx));
                ilGenerator
                    .Ldarg(1)
                    .Newobj(() => new ServiceReaderCtx(null))
                    .Castclass(typeof(IReaderCtx))
                    .Stloc(localReaderCtx);
            }
            for (int i = 0; i < methodInf.Parameters.Length; i++)
            {
                var fieldHandler = methodInf.Parameters[i].FieldHandler;
                localParams[i] = ilGenerator.DeclareLocal(methodInf.MethodInfo.GetParameters()[i].ParameterType);
                if (fieldHandler.NeedsCtx())
                {
                    fieldHandler.Load(ilGenerator, il => il.Ldloc(localReaderCtx));
                }
                else
                {
                    fieldHandler.Load(ilGenerator, il => il.Ldarg(1));
                }
                _typeConvertorGenerator.GenerateConversion(fieldHandler.HandledType(), localParams[i].LocalType)
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
            if (binding.OneWay)
            {
                if (returnType != typeof(void)) ilGenerator.Pop();
            }
            else
            {
                if (localResult == null)
                {
                    if (isAsync)
                    {
                        ilGenerator
                            .Ldarg(2)
                            .Ldloc(localResultId)
                            .Newobj(CreateTaskContinuationWithResultMarshaling(methodInf.MethodInfo.ReturnType, methodInf.ResultFieldHandler));
                    }
                    else
                    {
                        if (methodInf.MethodInfo.ReturnType != typeof(void)) ilGenerator.Pop();
                        ilGenerator
                            .Ldarg(2)
                            .Ldloc(localResultId)
                            .Callvirt(() => ((IServiceInternalServer)null).VoidResultMarshaling(0u));
                    }
                }
                else
                {
                    _typeConvertorGenerator.GenerateConversion(returnType, localResult.LocalType)(ilGenerator);
                    ilGenerator
                        .Stloc(localResult)
                        .Ldarg(2)
                        .Ldloc(localResultId)
                        .Callvirt(() => ((IServiceInternalServer)null).StartResultMarshaling(0u))
                        .Stloc(localWriter);
                    LocalBuilder localWriterCtx = null;
                    if (methodInf.ResultFieldHandler.NeedsCtx())
                    {
                        localWriterCtx = ilGenerator.DeclareLocal(typeof(IWriterCtx));
                        ilGenerator
                            .Ldloc(localWriter)
                            .Newobj(() => new ServiceWriterCtx(null))
                            .Castclass(typeof(IWriterCtx))
                            .Stloc(localWriterCtx);

                    }
                    methodInf.ResultFieldHandler.Save(ilGenerator, il => il.Ldloc(methodInf.ResultFieldHandler.NeedsCtx() ? localWriterCtx : localWriter), il => il.Ldloc(localResult));
                    ilGenerator
                        .Ldarg(2)
                        .Ldloc(localWriter)
                        .Callvirt(() => ((IServiceInternalServer)null).FinishResultMarshaling(null));
                }
                ilGenerator
                    .Catch(typeof(Exception))
                    .Stloc(localException)
                    .Ldarg(2)
                    .Ldloc(localResultId)
                    .Ldloc(localException)
                    .Callvirt(() => ((IServiceInternalServer)null).ExceptionMarshaling(0u, null))
                    .EndExceptionBlock();
            }
            ilGenerator.Ret();

            binding.Runner = method.Create();
            _serverBindings.TryAdd(binding.BindingId, binding);
        }

        ConstructorInfo CreateTaskContinuationWithResultMarshaling(Type taskType, IFieldHandler resultFieldHandler)
        {
            var name = "TaskContinuationWithResultMarshaling_" + taskType.Name;
            var ab = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name + "Asm"), AssemblyBuilderAccess.RunAndSave);
            var mb = ab.DefineDynamicModule(name + "Asm.dll", true);
            var symbolDocumentWriter = mb.DefineDocument("just_dynamic_" + name, Guid.Empty, Guid.Empty, Guid.Empty);
            var tb = mb.DefineType(name + "Impl", TypeAttributes.Public, typeof(object), Type.EmptyTypes);
            var ownerField = tb.DefineField("_owner", typeof(IServiceInternalServer), FieldAttributes.Private);
            var resultIdField = tb.DefineField("_resultId", typeof(uint), FieldAttributes.Private);
            var methodBuilder = tb.DefineMethod("Run", MethodAttributes.Public, typeof(void), new[] { taskType });
            var ilGenerator = methodBuilder.GetILGenerator(symbolDocumentWriter);
            ilGenerator.DeclareLocal(typeof(AbstractBufferedWriter));
            var notFaultedLabel = ilGenerator.DefineLabel();
            ilGenerator
                .Ldarg(1)
                .Callvirt(typeof(Task).GetMethod("get_IsFaulted"))
                .BrfalseS(notFaultedLabel)
                .Ldarg(0)
                .Ldfld(ownerField)
                .Ldarg(0)
                .Ldfld(resultIdField)
                .Ldarg(1)
                .Callvirt(typeof(Task).GetMethod("get_Exception"))
                .Callvirt(() => ((IServiceInternalServer)null).ExceptionMarshaling(0u, null))
                .Ret()
                .Mark(notFaultedLabel)
                .Ldarg(0)
                .Ldfld(ownerField)
                .Ldarg(0)
                .Ldfld(resultIdField);
            if (resultFieldHandler == null)
            {
                ilGenerator
                    .Callvirt(() => ((IServiceInternalServer)null).VoidResultMarshaling(0u));
            }
            else
            {
                ilGenerator
                    .Callvirt(() => ((IServiceInternalServer)null).StartResultMarshaling(0u))
                    .Stloc(0);
                resultFieldHandler.Save(ilGenerator, il => il.Ldloc(0), il =>
                    {
                        il.Ldarg(1).Callvirt(taskType.GetMethod("get_Result"));
                        _typeConvertorGenerator.GenerateConversion(taskType.UnwrapTask(), resultFieldHandler.HandledType())(il);
                    });
                ilGenerator
                    .Ldarg(0)
                    .Ldfld(ownerField)
                    .Ldloc(0)
                    .Callvirt(() => ((IServiceInternalServer)null).FinishResultMarshaling(null));
            }
            ilGenerator.Ret();
            var constructorBuilder = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard,
                                                          new[] { taskType, typeof(IServiceInternalServer), typeof(uint) });
            var actionOfTaskType = typeof(Action<>).MakeGenericType(taskType);
            ilGenerator = constructorBuilder.GetILGenerator();
            ilGenerator
                .Ldarg(0)
                .Call(() => new object())
                .Ldarg(0)
                .Ldarg(2)
                .Stfld(ownerField)
                .Ldarg(0)
                .Ldarg(3)
                .Stfld(resultIdField)
                .Ldarg(1)
                .Ldarg(0)
                .Ldftn(methodBuilder)
                .Newobj(actionOfTaskType.GetConstructor(new[] { typeof(object), typeof(IntPtr) }))
                .Callvirt(taskType.GetMethod("ContinueWith", new[] { actionOfTaskType }))
                .Pop()
                .Ret();
            var type = tb.CreateType();
            ab.Save(mb.ScopeName);
            return type.GetConstructors()[0];
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
            var weak = _otherServiceCache.GetValue(serviceType, t => new WeakReference(null, true));
            lock (weak)
            {
                var result = weak.Target;
                if (result == null)
                {
                    weak.Target = result = InternalQueryOtherService(serviceType);
                }
                return result;
            }
        }

        object InternalQueryOtherService(Type serviceType)
        {
            var typeInf = new TypeInf(serviceType, _fieldHandlerFactory);
            var bestMatch = int.MinValue;
            var bestServiceId = 0u;
            TypeInf bestServiceTypeInf = null;
            foreach (var servicesType in _clientKnownServicesTypes)
            {
                var targetTypeInf = _clientTypeInfs[servicesType.Value];
                var score = EvaluateCompatibility(typeInf, targetTypeInf, null);
                if (score > bestMatch)
                {
                    bestMatch = score;
                    bestServiceId = servicesType.Key;
                    bestServiceTypeInf = targetTypeInf;
                }
            }
            if (bestServiceTypeInf == null) return null;
            var mapping = new uint[typeInf.MethodInfs.Length][];
            EvaluateCompatibility(typeInf, bestServiceTypeInf, mapping);
            var name = serviceType.Name;
            var ab = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name + "Asm"), AssemblyBuilderAccess.RunAndSave);
            var mb = ab.DefineDynamicModule(name + "Asm.dll", true);
            var symbolDocumentWriter = mb.DefineDocument("just_dynamic_" + name, Guid.Empty, Guid.Empty, Guid.Empty);
            var isDelegate = serviceType.IsSubclassOf(typeof(Delegate));
            var tb = mb.DefineType(name + "Impl", TypeAttributes.Public, typeof(object), isDelegate ? Type.EmptyTypes : new[] { serviceType });
            var ownerField = tb.DefineField("_owner", typeof(IServiceInternalClient), FieldAttributes.Private);
            var bindings = new List<ClientBindInf>();
            var bindingFields = new List<FieldBuilder>();
            var bindingResultTypes = new List<string>();
            ILGenerator ilGenerator;
            for (int sourceMethodIndex = 0; sourceMethodIndex < typeInf.MethodInfs.Length; sourceMethodIndex++)
            {
                var methodInf = typeInf.MethodInfs[sourceMethodIndex];
                var methodInfo = methodInf.MethodInfo;
                var bindingField = tb.DefineField(string.Format("_b{0}", bindings.Count), typeof(ClientBindInf), FieldAttributes.Private);
                bindingFields.Add(bindingField);
                var parameterTypes = methodInfo.GetParameters().Select(pi => pi.ParameterType).ToArray();
                var returnType = methodInfo.ReturnType.UnwrapTask();
                var isAsync = returnType != methodInfo.ReturnType;
                var methodBuilder = tb.DefineMethod(methodInfo.Name, MethodAttributes.Public | MethodAttributes.Virtual,
                                                    methodInfo.ReturnType, parameterTypes);
                ilGenerator = methodBuilder.GetILGenerator(symbolDocumentWriter);
                var targetMethodInf = bestServiceTypeInf.MethodInfs[mapping[sourceMethodIndex][0]];
                var targetMethodIndex = Array.IndexOf(bestServiceTypeInf.MethodInfs, targetMethodInf);
                var bindingId = _clientBindNumbers.Allocate();
                Type resultAsTask;
                Type resultAsTcs;
                if (returnType != typeof(void))
                {
                    resultAsTask = typeof(Task<>).MakeGenericType(returnType);
                    resultAsTcs = typeof(TaskCompletionSource<>).MakeGenericType(returnType);
                }
                else
                {
                    resultAsTask = typeof(Task);
                    resultAsTcs = typeof(TaskCompletionSource<Unit>);
                }

                var bindingInf = new ClientBindInf
                    {
                        BindingId = bindingId,
                        ServiceId = bestServiceId,
                        MethodId = (uint)targetMethodIndex,
                        OneWay = !isAsync && returnType == typeof(void)
                    };
                _clientBindings.TryAdd(bindingId, bindingInf);
                bindings.Add(bindingInf);
                var writer = new ByteArrayWriter();
                writer.WriteVUInt32((uint)Command.Subcommand);
                writer.WriteVUInt32((uint)Subcommand.Bind);
                bindingInf.Store(writer);
                _channel.Send(ByteBuffer.NewAsync(writer.Data));
                LocalBuilder resultTaskLocal = null;
                if (!bindingInf.OneWay)
                {
                    resultTaskLocal = ilGenerator.DeclareLocal(typeof(Task));
                }
                var writerLocal = ilGenerator.DeclareLocal(typeof(AbstractBufferedWriter));
                ilGenerator
                    .Ldarg(0)
                    .Ldfld(ownerField)
                    .Ldarg(0)
                    .Ldfld(bindingField);
                if (bindingInf.OneWay)
                {
                    ilGenerator
                        .Callvirt(() => ((IServiceInternalClient)null).StartOneWayMarshaling(null));
                }
                else
                {
                    Task placebo;
                    ilGenerator
                        .Ldloca(resultTaskLocal)
                        .Callvirt(() => ((IServiceInternalClient)null).StartTwoWayMarshaling(null, out placebo));
                }
                ilGenerator.Stloc(writerLocal);
                var needsCtx = targetMethodInf.Parameters.Any(p => p.FieldHandler.NeedsCtx());
                LocalBuilder writerCtxLocal = null;
                if (needsCtx)
                {
                    writerCtxLocal = ilGenerator.DeclareLocal(typeof(IWriterCtx));
                    ilGenerator
                        .Ldloc(writerLocal)
                        .Newobj(() => new ServiceWriterCtx(null))
                        .Castclass(typeof(IWriterCtx))
                        .Stloc(writerCtxLocal);
                }
                for (int paramOrder = 0; paramOrder < targetMethodInf.Parameters.Length; paramOrder++)
                {
                    var parameterInf = targetMethodInf.Parameters[paramOrder];
                    var sourceParamIndex = mapping[sourceMethodIndex][paramOrder + 1];
                    Action<ILGenerator> pushWriterOrCtx;
                    if (needsCtx) pushWriterOrCtx = il => il.Ldloc(writerCtxLocal);
                    else pushWriterOrCtx = il => il.Ldloc(writerLocal);
                    if (sourceParamIndex == uint.MaxValue)
                    {
                        if (parameterInf.FieldHandler.HandledType().IsValueType)
                        {
                            parameterInf.FieldHandler.Save(ilGenerator, pushWriterOrCtx, il =>
                            {
                                il.LdcI4(0);
                                _typeConvertorGenerator.GenerateConversion(typeof(int), parameterInf.FieldHandler.HandledType())(il);
                            });
                        }
                        else
                        {
                            parameterInf.FieldHandler.Save(ilGenerator, pushWriterOrCtx, il => il.Ldnull());
                        }
                    }
                    else
                    {
                        var convGen = _typeConvertorGenerator.GenerateConversion(parameterTypes[sourceParamIndex],
                                                                                 parameterInf.FieldHandler.HandledType());
                        parameterInf.FieldHandler.Save(ilGenerator, pushWriterOrCtx, il =>
                        {
                            il.Ldarg((ushort)(sourceParamIndex + 1));
                            convGen(il);
                        });
                    }
                }
                ilGenerator
                    .Ldarg(0)
                    .Ldfld(ownerField)
                    .Ldloc(writerLocal);
                if (bindingInf.OneWay)
                    ilGenerator.Callvirt(() => ((IServiceInternalClient)null).FinishOneWayMarshaling(null));
                else
                {
                    ilGenerator
                        .Callvirt(() => ((IServiceInternalClient)null).FinishTwoWayMarshaling(null))
                        .Ldloc(resultTaskLocal)
                        .Castclass(resultAsTask);
                    if (!isAsync)
                        ilGenerator.Callvirt(resultAsTask.GetMethod("get_Result"));
                }
                ilGenerator.Ret();
                if (!isDelegate)
                    tb.DefineMethodOverride(methodBuilder, methodInfo);
                if (bindingInf.OneWay)
                {
                    bindingResultTypes.Add("");
                    continue;
                }
                if (bindingResultTypes.Contains(returnType.FullName))
                {
                    bindingResultTypes.Add(returnType.FullName);
                    continue;
                }
                bindingResultTypes.Add(returnType.FullName);
                methodBuilder = tb.DefineMethod("HandleResult_" + returnType.FullName,
                                                MethodAttributes.Public | MethodAttributes.Static, typeof(void),
                                                new[] { typeof(object), typeof(AbstractBufferedReader) });
                ilGenerator = methodBuilder.GetILGenerator(symbolDocumentWriter);
                ilGenerator
                    .Ldarg(0)
                    .Castclass(resultAsTcs);
                if (targetMethodInf.ResultFieldHandler == null && returnType == typeof(void))
                {
                    ilGenerator.Ldnull();
                }
                else
                {
                    if (targetMethodInf.ResultFieldHandler.NeedsCtx())
                    {
                        var readerCtxLocal = ilGenerator.DeclareLocal(typeof(IReaderCtx));
                        ilGenerator
                            .Ldarg(1)
                            .Newobj(() => new ServiceReaderCtx(null))
                            .Castclass(typeof(IReaderCtx))
                            .Stloc(readerCtxLocal);
                        targetMethodInf.ResultFieldHandler.Load(ilGenerator, il => il.Ldloc(readerCtxLocal));
                    }
                    else
                    {
                        targetMethodInf.ResultFieldHandler.Load(ilGenerator, il => il.Ldarg(1));
                    }
                    _typeConvertorGenerator.GenerateConversion(targetMethodInf.ResultFieldHandler.HandledType(), returnType)
                        (ilGenerator);
                }
                ilGenerator
                    .Callvirt(resultAsTcs.GetMethod("TrySetResult"))
                    .Pop()
                    .Ret();

                methodBuilder = tb.DefineMethod("HandleException_" + returnType.FullName,
                                                MethodAttributes.Public | MethodAttributes.Static, typeof(void),
                                                new[] { typeof(object), typeof(Exception) });
                ilGenerator = methodBuilder.GetILGenerator(symbolDocumentWriter, 16);
                ilGenerator
                    .Ldarg(0)
                    .Castclass(resultAsTcs)
                    .Ldarg(1)
                    .Callvirt(resultAsTcs.GetMethod("TrySetException", new[] { typeof(Exception) }))
                    .Pop()
                    .Ret();

                methodBuilder = tb.DefineMethod("HandleCancellation_" + returnType.FullName,
                                                MethodAttributes.Public | MethodAttributes.Static, typeof(void),
                                                new[] { typeof(object) });
                ilGenerator = methodBuilder.GetILGenerator(symbolDocumentWriter, 16);
                ilGenerator
                    .Ldarg(0)
                    .Castclass(resultAsTcs)
                    .Callvirt(resultAsTcs.GetMethod("TrySetCanceled", Type.EmptyTypes))
                    .Pop()
                    .Ret();

                methodBuilder = tb.DefineMethod("TaskWithSourceCreator_" + returnType.FullName,
                                                MethodAttributes.Public | MethodAttributes.Static,
                                                typeof(TaskWithSource), Type.EmptyTypes);
                ilGenerator = methodBuilder.GetILGenerator(symbolDocumentWriter, 32);
                ilGenerator.DeclareLocal(resultAsTcs);
                ilGenerator
                    .Newobj(resultAsTcs.GetConstructor(Type.EmptyTypes))
                    .Stloc(0)
                    .Ldloc(0)
                    .Ldloc(0)
                    .Callvirt(resultAsTcs.GetMethod("get_Task"))
                    .Newobj(() => new TaskWithSource(null, null))
                    .Ret();
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
            ab.Save(mb.ScopeName);
            for (int i = 0; i < bindings.Count; i++)
            {
                var resultType = bindingResultTypes[i];
                if (resultType == "") continue;
                bindings[i].HandleResult =
                    finalType.GetMethod("HandleResult_" + resultType).CreateDelegate<Action<object, AbstractBufferedReader>>();
                bindings[i].HandleException =
                    finalType.GetMethod("HandleException_" + resultType).CreateDelegate<Action<object, Exception>>();
                bindings[i].HandleCancellation =
                    finalType.GetMethod("HandleCancellation_" + resultType).CreateDelegate<Action<object>>();
                bindings[i].TaskWithSourceCreator =
                    finalType.GetMethod("TaskWithSourceCreator_" + resultType).CreateDelegate<Func<TaskWithSource>>();
            }
            var finalObject = finalType.GetConstructor(constructorParams).Invoke(new object[] { this, bindings.ToArray() });
            return isDelegate ? Delegate.CreateDelegate(serviceType, finalObject, "Invoke") : finalObject;
        }

        int EvaluateCompatibility(TypeInf from, TypeInf to, uint[][] mapping)
        {
            var result = 0;
            if (from.MethodInfs.Length == 1 && to.MethodInfs.Length == 1)
            {
                uint[] parameterMapping = null;
                if (mapping != null)
                {
                    parameterMapping = new uint[1 + to.MethodInfs[0].Parameters.Length];
                    mapping[0] = parameterMapping;
                }
                result = EvaluateCompatibilityIgnoringName(from.MethodInfs[0], to.MethodInfs[0], parameterMapping);
                if (result == int.MinValue) return result;
                if (from.Name == to.Name) result += 5;
                return result;
            }
            for (int i = 0; i < from.MethodInfs.Length; i++)
            {
                var methodToCheck = from.MethodInfs[i];
                var bestValue = int.MinValue;
                var bestIndex = int.MinValue;
                for (int j = 0; j < to.MethodInfs.Length; j++)
                {
                    if (methodToCheck.Name != to.MethodInfs[j].Name) continue;
                    var value = EvaluateCompatibilityIgnoringName(methodToCheck, to.MethodInfs[j], null);
                    if (value > bestValue)
                    {
                        bestValue = value;
                        bestIndex = j;
                    }
                    else if (value == bestValue)
                    {
                        bestIndex = int.MinValue; // Ambiguity
                    }
                }
                if (bestIndex == int.MinValue) return int.MinValue;
                result += bestValue;
                if (mapping != null)
                {
                    var parameterMapping = new uint[1 + to.MethodInfs[bestIndex].Parameters.Length];
                    parameterMapping[0] = (uint)bestIndex;
                    EvaluateCompatibilityIgnoringName(methodToCheck, to.MethodInfs[bestIndex], parameterMapping);
                    mapping[i] = parameterMapping;
                }
            }
            return result;
        }

        int EvaluateCompatibilityIgnoringName(MethodInf from, MethodInf to, uint[] mapping)
        {
            var result = 0;
            if (from.ResultFieldHandler != null && to.ResultFieldHandler != null)
            {
                result = EvaluateCompatibility(to.ResultFieldHandler, from.ResultFieldHandler); // from to is exchanged because return value going back
                if (result == int.MinValue) return result;
            }
            if (mapping != null)
                for (int i = 1; i < mapping.Length; i++)
                {
                    mapping[i] = uint.MaxValue;
                }
            var usedFrom = new bool[from.Parameters.Length];
            var usedTo = new bool[to.Parameters.Length];
            for (int i = 0; i < from.Parameters.Length; i++)
            {
                var name = from.Parameters[i].Name;
                for (int j = 0; j < to.Parameters.Length; j++)
                {
                    if (usedTo[j]) continue;
                    if (name == to.Parameters[j].Name)
                    {
                        var value = EvaluateCompatibility(from.Parameters[i].FieldHandler, to.Parameters[j].FieldHandler);
                        if (value == int.MinValue) return int.MinValue;
                        usedFrom[i] = true;
                        usedTo[j] = true;
                        if (mapping != null) mapping[j + 1] = (uint)i;
                        result += value + 1;
                        break;
                    }
                }
            }
            for (int i = 0; i < from.Parameters.Length; i++)
            {
                if (usedFrom[i]) continue;
                for (int j = 0; j < to.Parameters.Length; j++)
                {
                    if (usedTo[j]) continue;
                    var value = EvaluateCompatibility(from.Parameters[i].FieldHandler, to.Parameters[j].FieldHandler);
                    if (value == int.MinValue)
                    {
                        for (; i < from.Parameters.Length; i++)
                            if (!usedFrom[i]) result--;
                        return result;
                    }
                    usedFrom[i] = true;
                    usedTo[j] = true;
                    if (mapping != null) mapping[j + 1] = (uint)i;
                    result += value;
                    break;
                }
                if (!usedFrom[i]) result--;
            }
            return result;
        }

        int EvaluateCompatibility(IFieldHandler from, IFieldHandler to)
        {
            if (from.Name == to.Name && (from.Configuration == to.Configuration || from.Configuration.SequenceEqual(to.Configuration))) return 10;
            var typeFrom = from.HandledType();
            var typeTo = to.HandledType();
            if (_typeConvertorGenerator.GenerateConversion(typeFrom, typeTo) != null) return 5;
            return int.MinValue;
        }

        public void RegisterMyService(object service)
        {
            if (service == null) throw new ArgumentNullException("service");
            var serviceId = _serverObjectNumbers.Allocate();
            _serverObjects.TryAdd(serviceId, service);
            _serverServices.TryAdd(service, serviceId);
            Type type = service.GetType();
            var typeId = _serverTypeNumbers.Allocate();
            var typeInf = new TypeInf(type, _fieldHandlerFactory);
            _serverTypeInfs.TryAdd(typeId, typeInf);
            _serverKnownServicesTypes.TryAdd(serviceId, typeId);
            var writer = new ByteArrayWriter();
            writer.WriteVUInt32((uint)Command.Subcommand);
            writer.WriteVUInt32((uint)Subcommand.RegisterType);
            writer.WriteVUInt32(typeId);
            typeInf.Store(writer);
            _channel.Send(ByteBuffer.NewAsync(writer.Data));
            writer = new ByteArrayWriter();
            writer.WriteVUInt32((uint)Command.Subcommand);
            writer.WriteVUInt32((uint)Subcommand.RegisterService);
            writer.WriteVUInt32(serviceId);
            writer.WriteVUInt32(typeId);
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
                _channel.Send(ByteBuffer.NewAsync(writer.Data));
            }
        }

        public IChannel Channel
        {
            get { return _channel; }
        }

        public ITypeConvertorGenerator TypeConvertorGenerator
        {
            get { return _typeConvertorGenerator; }
            set { _typeConvertorGenerator = value; }
        }

        public IFieldHandlerFactory FieldHandlerFactory
        {
            get { return _fieldHandlerFactory; }
            set { _fieldHandlerFactory = value; }
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
            _channel.Send(ByteBuffer.NewAsync(((ByteArrayWriter)writer).Data));
        }

        public AbstractBufferedWriter StartOneWayMarshaling(ClientBindInf binding)
        {
            var message = new ByteArrayWriter();
            message.WriteVUInt32(binding.BindingId);
            return message;
        }

        public void FinishOneWayMarshaling(AbstractBufferedWriter writer)
        {
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
            _channel.Send(ByteBuffer.NewAsync(((ByteArrayWriter)writer).Data));
        }

        public void ExceptionMarshaling(uint resultId, Exception ex)
        {
            var message = new ByteArrayWriter();
            message.WriteVUInt32((uint)Command.Exception);
            message.WriteVUInt32(resultId);
            using (var stream = new MemoryStream())
            {
                new BinaryFormatter().Serialize(stream, ex);
                message.WriteByteArray(stream.ToArray());
            }
            _channel.Send(ByteBuffer.NewAsync(message.Data));
        }

        public void VoidResultMarshaling(uint resultId)
        {
            var message = new ByteArrayWriter();
            message.WriteVUInt32((uint)Command.Result);
            message.WriteVUInt32(resultId);
            _channel.Send(ByteBuffer.NewAsync(message.Data));
        }

        public string RegisterType(Type type)
        {
            throw new NotImplementedException();
        }

        public Type TypeByName(string name)
        {
            throw new NotImplementedException();
        }
    }
}
