using System;
using System.Security.Cryptography;
using BTDB.Collections;
using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public class DBReaderCtx : IDBReaderCtx
{
    protected readonly IInternalObjectDBTransaction? Transaction;
    StructList<object> _objects;
    StructList<long> _returningStack;
    int _lastIdOfObj;

    public DBReaderCtx(IInternalObjectDBTransaction? transaction)
    {
        Transaction = transaction;
        _lastIdOfObj = -1;
    }

    public bool ReadObject(ref SpanReader reader, out object? @object)
    {
        var id = reader.ReadVInt64();
        if (id == 0)
        {
            @object = null;
            return false;
        }

        if (id <= int.MinValue || id > 0)
        {
            @object = Transaction!.Get((ulong)id);
            return false;
        }

        var ido = (int)(-id) - 1;
        var o = RetrieveObj(ido);
        if (o != null)
        {
            if (!(o is IMemorizedPosition mp))
            {
                @object = o;
                return false;
            }

            PushReturningPosition(reader.GetCurrentPosition());
            mp.Restore(ref reader);
        }
        else
        {
            PushReturningPosition(-1);
        }

        _lastIdOfObj = ido;
        @object = null;
        return true;
    }

    void PushReturningPosition(long memorizedPosition)
    {
        if (_returningStack.Count == 0 && memorizedPosition < 0) return;
        _returningStack.Add(memorizedPosition);
    }

    public void RegisterObject(object @object)
    {
        _objects![_lastIdOfObj] = @object;
    }

    public void ReadObjectDone(ref SpanReader reader)
    {
        if (_returningStack.Count == 0) return;
        var returnPos = _returningStack[^1];
        _returningStack.Pop();
        if (returnPos >= 0) reader.SetCurrentPosition(returnPos);
    }

    public object? ReadNativeObject(ref SpanReader reader)
    {
        var test = ReadObject(ref reader, out var @object);
        if (test)
        {
            @object = Transaction!.ReadInlineObject(ref reader, this, false);
        }

        return @object;
    }

    public bool SkipObject(ref SpanReader reader)
    {
        var id = reader.ReadVInt64();
        if (id == 0)
        {
            return false;
        }

        if (id <= int.MinValue || id > 0)
        {
            return false;
        }

        var ido = (int)(-id) - 1;
        var o = RetrieveObj(ido);
        if (o != null)
        {
            return false;
        }

        _objects[ido] = new MemorizedPosition(reader.GetCurrentPosition());
        _lastIdOfObj = ido;
        return true;
    }

    public void SkipNativeObject(ref SpanReader reader)
    {
        var test = SkipObject(ref reader);
        if (test)
        {
            Transaction!.ReadInlineObject(ref reader, this, true);
        }
    }

    object? RetrieveObj(int ido)
    {
        while (_objects.Count <= ido) _objects.Add(null);
        return _objects[ido];
    }

    public EncryptedString ReadEncryptedString(ref SpanReader reader)
    {
        var cipher = Transaction!.Owner.GetSymmetricCipher();
        var enc = reader.ReadByteArray();
        var size = cipher.CalcPlainSizeFor(enc);
        var dec = new byte[size];
        if (!cipher.Decrypt(enc, dec))
        {
            throw new CryptographicException();
        }

        var r = new SpanReader(dec.AsSpan());
        return r.ReadString();
    }

    public void SkipEncryptedString(ref SpanReader reader)
    {
        reader.SkipByteArray();
    }

    public EncryptedString ReadOrderedEncryptedString(ref SpanReader reader)
    {
        var cipher = Transaction!.Owner.GetSymmetricCipher();
        var enc = reader.ReadByteArray();
        var size = cipher.CalcOrderedPlainSizeFor(enc);
        var dec = new byte[size];
        if (!cipher.OrderedDecrypt(enc, dec))
        {
            throw new CryptographicException();
        }

        var r = new SpanReader(dec.AsSpan());
        return r.ReadString();
    }

    public void SkipOrderedEncryptedString(ref SpanReader reader)
    {
        reader.SkipByteArray();
    }

    public IInternalObjectDBTransaction? GetTransaction()
    {
        return Transaction;
    }

    public virtual void RegisterDict(ulong dictId)
    {
    }

    public virtual void RegisterOid(ulong oid)
    {
    }

    public virtual void FreeContentInNativeObject(ref SpanReader reader)
    {
    }
}
