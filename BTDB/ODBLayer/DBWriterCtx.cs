using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
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

    public bool WriteObject(ref MemWriter writer, object @object)
    {
        return CommonWriteObject(ref writer, @object, false, true);
    }

    bool CommonWriteObject(ref MemWriter writer, object? @object, bool autoRegister, bool forceInline)
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

    public void WriteNativeObject(ref MemWriter writer, object @object)
    {
        if (!CommonWriteObject(ref writer, @object, true, true)) return;
        _transaction.WriteInlineObject(ref writer, @object, this);
    }

    public void WriteNativeObjectPreventInline(ref MemWriter writer, object @object)
    {
        if (!CommonWriteObject(ref writer, @object, true, false)) return;
        _transaction.WriteInlineObject(ref writer, @object, this);
    }

    [SkipLocalsInit]
    public void WriteEncryptedString(ref MemWriter outerWriter, EncryptedString value)
    {
        Span<byte> buf = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteString(value);
        var cipher = _transaction.Owner.GetSymmetricCipher();
        var plain = writer.GetSpan();
        var encSize = cipher.CalcEncryptedSizeFor(plain);
        outerWriter.WriteVUInt32(1u + (uint)encSize);
        var enc = outerWriter.BlockWriteToSpan(encSize, out var spanNeedToBeWritten);
        cipher.Encrypt(plain, enc);
        if (spanNeedToBeWritten)
            outerWriter.WriteBlock(enc);
    }

    [SkipLocalsInit]
    public void WriteOrderedEncryptedString(ref MemWriter outerWriter, EncryptedString value)
    {
        Span<byte> buf = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteString(value);
        var cipher = _transaction.Owner.GetSymmetricCipher();
        var plain = writer.GetSpan();
        var encSize = cipher.CalcOrderedEncryptedSizeFor(plain);
        outerWriter.WriteVUInt32(1u + (uint)encSize);
        var enc = outerWriter.BlockWriteToSpan(encSize, out var spanNeedToBeWritten);
        cipher.OrderedEncrypt(plain, enc);
        if (spanNeedToBeWritten)
            outerWriter.WriteBlock(enc);
    }

    public IInternalObjectDBTransaction GetTransaction()
    {
        return _transaction;
    }
}
