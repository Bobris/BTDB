using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.Reactive;

namespace BTDB.ServiceLayer
{
    public class Service : IService
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
        readonly Dictionary<uint,TypeInf> _serverTypeInfs = new Dictionary<uint, TypeInf>();
        readonly NumberAllocator _serverBindNumbers = new NumberAllocator((uint) Command.FirstToBind);

        readonly Dictionary<uint,TypeInf> _clientTypeInfs = new Dictionary<uint, TypeInf>();
        readonly Dictionary<uint,uint> _clientKnownServicesTypes = new Dictionary<uint, uint>();
        readonly Dictionary<uint,BindInf> _clientBindings = new Dictionary<uint, BindInf>();

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
            switch((Command)c0)
            {
                case Command.Subcommand:
                    OnSubcommand(reader);
                    break;
                case Command.Result:
                    break;
                case Command.Exception:
                    break;
                case Command.FirstToBind:
                    break;
                default:
                    break;
            }
        }

        void OnSubcommand(ByteBufferReader reader)
        {
            var c1 = reader.ReadVUInt32();
            switch((Subcommand)c1)
            {
                case Subcommand.RegisterType:
                    var typeId = reader.ReadVUInt32();
                    _clientTypeInfs.Add(typeId, new TypeInf(reader));
                    break;
                case Subcommand.RegisterService:
                    var serviceId = reader.ReadVUInt32();
                    _clientKnownServicesTypes.Add(serviceId,reader.ReadVUInt32());
                    break;
                case Subcommand.UnregisterService:
                    OnUnregisterService(reader.ReadVUInt32());
                    break;
                case Subcommand.Bind:
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
            return (T) QueryOtherService(typeof (T));
        }

        public object QueryOtherService(Type serviceType)
        {
            if (serviceType == null) throw new ArgumentNullException("serviceType");
            throw new NotImplementedException();
        }

        public void RegisterMyService(object service)
        {
            if (service == null) throw new ArgumentNullException("service");
            lock(_serverServiceLock)
            {
                var serviceId = ++_lastServerServiceId;
                _serverServices.Add(service, serviceId);
                Type type = service.GetType();
                var typeId = _serverTypeNumbers.Allocate();
                var typeInf = new TypeInf(type);
                _serverTypeInfs.Add(typeId, typeInf);
                var writer = new ByteArrayWriter();
                writer.WriteVUInt32((uint) Command.Subcommand);
                writer.WriteVUInt32((uint) Subcommand.RegisterType);
                writer.WriteVUInt32(typeId);
                typeInf.Store(writer);
                writer.Dispose();
                _channel.Send(ByteBuffer.NewAsync(writer.Data));
                writer = new ByteArrayWriter();
                writer.WriteVUInt32((uint) Command.Subcommand);
                writer.WriteVUInt32((uint) Subcommand.RegisterService);
                writer.WriteVUInt32((uint) serviceId);
                writer.WriteVUInt32(typeId);
                writer.Dispose();
                _channel.Send(ByteBuffer.NewAsync(writer.Data));
            }
        }

        public void UnregisterMyService(object service)
        {
            lock(_serverServiceLock)
            {
                ulong serviceId;
                if (_serverServices.TryGetValue(service, out serviceId))
                {
                    _serverServices.Remove(service);
                    var writer = new ByteArrayWriter();
                    writer.WriteVUInt32((uint) Command.Subcommand);
                    writer.WriteVUInt32((uint) Subcommand.UnregisterService);
                    writer.WriteVUInt32((uint) serviceId);
                    writer.Dispose();
                    _channel.Send(ByteBuffer.NewAsync(writer.Data));
                }
            }
        }

        public IChannel Channel
        {
            get { return _channel; }
        }
    }

    internal class BindInf
    {
        internal uint ServiceId { get; set; }
        internal uint MethodId { get; set; }
        internal bool OneWay { get; set; }
    }
}
