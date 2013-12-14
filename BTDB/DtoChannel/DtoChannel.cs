using System;
using System.IO;
using BTDB.Buffer;
using BTDB.EventStoreLayer;
using BTDB.Reactive;
using BTDB.Service;
using BTDB.StreamLayer;

namespace BTDB.DtoChannel
{
    public class DtoChannel : IDtoChannel
    {
        readonly IChannel _channel;
        readonly ITypeSerializersMapping _mapping;
        readonly FastSubject<object> _onReceive = new FastSubject<object>();

        public DtoChannel(IChannel channel, ITypeSerializersMapping mapping)
        {
            _channel = channel;
            _mapping = mapping;
            _channel.OnReceive.Subscribe(new Receiver(this));
        }

        class Receiver : IObserver<ByteBuffer>
        {
            readonly DtoChannel _dtoChannel;

            public Receiver(DtoChannel dtoChannel)
            {
                _dtoChannel = dtoChannel;
            }

            public void OnNext(ByteBuffer value)
            {
                var reader = new ByteBufferReader(value);
                byte c0 = 0;
                if (!reader.Eof)
                    c0 = reader.ReadUInt8();
                if (c0 == 99)
                {
                    _dtoChannel._mapping.LoadTypeDescriptors(reader);
                }
                else if (c0 != 100)
                {
                    _dtoChannel._onReceive.OnError(new InvalidDataException("Data received from other side must start with byte 99 or 100"));
                    return;
                }
                _dtoChannel._onReceive.OnNext(_dtoChannel._mapping.LoadObject(reader));
            }

            public void OnError(Exception error)
            {
                _dtoChannel._onReceive.OnError(error);
            }

            public void OnCompleted()
            {
                _dtoChannel._onReceive.OnCompleted();
            }
        }

        public void Send(object dto)
        {
            IDescriptorSerializerContext serializerContext = _mapping;
            var writer = new ByteBufferWriter();
            writer.WriteUInt8(100);
            serializerContext = serializerContext.StoreNewDescriptors(writer, dto);
            serializerContext.FinishNewDescriptors(writer);
            serializerContext.StoreObject(writer,dto);
            var block = writer.Data;
            if (serializerContext.SomeTypeStored)
            {
                block[0] = 99;
            }
            _channel.Send(block);
            serializerContext.CommitNewDescriptors();
        }

        public IObservable<object> OnReceive
        {
            get { return _onReceive; }
        }

        public void Dispose()
        {
            _channel.Dispose();
        }
    }
}