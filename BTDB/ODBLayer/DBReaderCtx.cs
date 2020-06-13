using System.Diagnostics;
using System.Security.Cryptography;
using BTDB.Collections;
using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class DBReaderCtx : IDBReaderCtx
    {
        protected readonly IInternalObjectDBTransaction Transaction;
        protected readonly AbstractBufferedReader? _reader;
        StructList<object> _objects;
        StructList<IMemorizedPosition?> _returningStack;
        int _lastIdOfObj;

        public DBReaderCtx(IInternalObjectDBTransaction transaction, AbstractBufferedReader reader)
        {
            Transaction = transaction;
            _reader = reader;
            _lastIdOfObj = -1;
        }

        public DBReaderCtx(IInternalObjectDBTransaction transaction)
        {
            Transaction = transaction;
            _reader = null;
            _lastIdOfObj = -1;
        }

        public bool ReadObject(out object? @object)
        {
            var id = _reader!.ReadVInt64();
            if (id == 0)
            {
                @object = null;
                return false;
            }

            if (id <= int.MinValue || id > 0)
            {
                @object = Transaction.Get((ulong) id);
                return false;
            }

            var ido = (int) (-id) - 1;
            var o = RetrieveObj(ido);
            if (o != null)
            {
                if (!(o is IMemorizedPosition mp))
                {
                    @object = o;
                    return false;
                }

                PushReturningPosition(((ICanMemorizePosition) _reader).MemorizeCurrentPosition());
                mp.Restore();
            }
            else
            {
                PushReturningPosition(null);
            }

            _lastIdOfObj = ido;
            @object = null;
            return true;
        }

        void PushReturningPosition(IMemorizedPosition? memorizedPosition)
        {
            if (_returningStack.Count == 0 && memorizedPosition == null) return;
            _returningStack.Add(memorizedPosition);
        }

        public void RegisterObject(object @object)
        {
            Debug.Assert(@object != null);
            _objects![_lastIdOfObj] = @object;
        }

        public void ReadObjectDone()
        {
            if (_returningStack.Count == 0) return;
            var returnPos = _returningStack[^1];
            _returningStack.RemoveAt(^1);
            returnPos?.Restore();
        }

        public object? ReadNativeObject()
        {
            var test = ReadObject(out var @object);
            if (test)
            {
                @object = Transaction.ReadInlineObject(this);
            }

            return @object;
        }

        public bool SkipObject()
        {
            var id = _reader!.ReadVInt64();
            if (id == 0)
            {
                return false;
            }

            if (id <= int.MinValue || id > 0)
            {
                return false;
            }

            var ido = (int) (-id) - 1;
            var o = RetrieveObj(ido);
            if (o != null)
            {
                return false;
            }

            _objects[ido] = ((ICanMemorizePosition) _reader).MemorizeCurrentPosition();
            _lastIdOfObj = ido;
            return true;
        }

        public void SkipNativeObject()
        {
            var test = SkipObject();
            if (test)
            {
                // This should be skip inline object, but it is easier just to throw away result
                Transaction.ReadInlineObject(this);
            }
        }

        object? RetrieveObj(int ido)
        {
            while (_objects.Count <= ido) _objects.Add(null);
            return _objects[ido];
        }

        public AbstractBufferedReader Reader()
        {
            return _reader!;
        }

        public EncryptedString ReadEncryptedString()
        {
            var cipher = Transaction.Owner.GetSymmetricCipher();
            var enc = Reader().ReadByteArray();
            var size = cipher.CalcPlainSizeFor(enc);
            var dec = new byte[size];
            if (!cipher.Decrypt(enc, dec))
            {
                throw new CryptographicException();
            }

            var r = new ByteArrayReader(dec);
            return r.ReadString();
        }

        public void SkipEncryptedString()
        {
            Reader().SkipByteArray();
        }

        public EncryptedString ReadOrderedEncryptedString()
        {
            var cipher = Transaction.Owner.GetSymmetricCipher();
            var enc = Reader().ReadByteArray();
            var size = cipher.CalcOrderedPlainSizeFor(enc);
            var dec = new byte[size];
            if (!cipher.OrderedDecrypt(enc, dec))
            {
                throw new CryptographicException();
            }

            var r = new ByteArrayReader(dec);
            return r.ReadString();
        }

        public void SkipOrderedEncryptedString()
        {
            Reader().SkipByteArray();
        }

        public IInternalObjectDBTransaction GetTransaction()
        {
            return Transaction;
        }

        public virtual void RegisterDict(ulong dictId)
        {
        }

        public virtual void RegisterOid(ulong oid)
        {
        }

        public virtual void FreeContentInNativeObject()
        {
        }
    }
}
