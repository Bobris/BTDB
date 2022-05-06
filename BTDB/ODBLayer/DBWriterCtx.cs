using System.Collections.Generic;
using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public class DBWriterCtx : IDBWriterCtx
{
    readonly IInternalObjectDBTransaction _transaction;
    Dictionary<object, int>? _objectIdMap;
    int _lastId;

    public DBWriterCtx(IInternalObjectDBTransaction transaction)
    {
        _transaction = transaction;
    }

    public bool WriteObject(ref SpanWriter writer, object @object)
    {
        return CommonWriteObject(ref writer, @object, false, true);
    }

    bool CommonWriteObject(ref SpanWriter writer, object? @object, bool autoRegister, bool forceInline)
    {
        if (@object == null)
        {
            writer.WriteVInt64(0);
            return false;
        }
        var oid = _transaction.StoreIfNotInlined(@object, autoRegister, forceInline);
        if (oid != ulong.MaxValue)
        {
            writer.WriteVInt64((long)oid);
            return false;
        }
        _objectIdMap ??= new(ReferenceEqualityComparer<object>.Instance);
        if (_objectIdMap.TryGetValue(@object, out var cid))
        {
            writer.WriteVInt64(-cid);
            return false;
        }
        _lastId++;
        _objectIdMap.Add(@object, _lastId);
        writer.WriteVInt64(-_lastId);
        return true;
    }

    public void WriteNativeObject(ref SpanWriter writer, object @object)
    {
        if (!CommonWriteObject(ref writer, @object, true, true)) return;
        _transaction.WriteInlineObject(ref writer, @object, this);
    }

    public void WriteNativeObjectPreventInline(ref SpanWriter writer, object @object)
    {
        if (!CommonWriteObject(ref writer, @object, true, false)) return;
        _transaction.WriteInlineObject(ref writer, @object, this);
    }

    public void WriteEncryptedString(ref SpanWriter outerWriter, EncryptedString value)
    {
        var writer = new SpanWriter();
        writer.WriteString(value);
        var cipher = _transaction.Owner.GetSymmetricCipher();
        var plain = writer.GetSpan();
        var encSize = cipher.CalcEncryptedSizeFor(plain);
        var enc = new byte[encSize];
        cipher.Encrypt(plain, enc);
        outerWriter.WriteByteArray(enc);
    }

    public void WriteOrderedEncryptedString(ref SpanWriter outerWriter, EncryptedString value)
    {
        var writer = new SpanWriter();
        writer.WriteString(value);
        var cipher = _transaction.Owner.GetSymmetricCipher();
        var plain = writer.GetSpan();
        var encSize = cipher.CalcOrderedEncryptedSizeFor(plain);
        var enc = new byte[encSize];
        cipher.OrderedEncrypt(plain, enc);
        outerWriter.WriteByteArray(enc);
    }

    public IInternalObjectDBTransaction GetTransaction()
    {
        return _transaction;
    }
}
