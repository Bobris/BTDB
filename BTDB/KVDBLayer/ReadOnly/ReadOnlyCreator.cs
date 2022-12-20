using System;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer.ReadOnly;

public class ReadOnlyCreator
{
    readonly IKeyValueDBTransaction _transaction;
    uint _maxChildren = 31;

    public ReadOnlyCreator(IKeyValueDBTransaction transaction)
    {
        _transaction = transaction;
    }

    public void SetMaxChildren(uint maxChildren)
    {
        if (maxChildren is < 2 or > 256) throw new ArgumentOutOfRangeException(nameof(maxChildren));
        _maxChildren = maxChildren;
    }

    public void Run(ref SpanWriter writer)
    {
        WriteHeader(ref writer);
        var valueOffsets = new uint[_transaction.GetKeyValueCount()];
        if (_transaction.FindFirstKey(default))
        {
            var i = 0;
            do
            {
                var val = _transaction.GetValue();
                if (val.Length == 0)
                {
                    valueOffsets[i++] = 0;
                }
                else
                {
                    var ofs = writer.GetCurrentPosition();
                    if (ofs > uint.MaxValue) throw new BTDBException("Values do not fit into 4GB");
                    valueOffsets[i++] = (uint)ofs;
                    writer.WriteVUInt32((uint)val.Length);
                    writer.WriteBlock(val);
                }
            } while (_transaction.FindNextKey(default));
        }

        var rootOffset = BuildTreeNode(_transaction.GetKeyValueCount(), valueOffsets, ref writer);
        if (rootOffset > uint.MaxValue) throw new BTDBException("RootOffset do not fit into 4GB");
        writer.WriteUInt32LE((uint)rootOffset);
    }

    ulong BuildTreeNode(long keyCount, uint[] valueOffsets, ref SpanWriter writer)
    {
        var leafs = (keyCount + _maxChildren - 1) / _maxChildren;
        var order = 0L;
        var done = 0L;
        var keys = new byte[_maxChildren][];
        return BuildBranchNode(leafs, ref writer, (ref SpanWriter writer) =>
        {
            order++;
            var reach = keyCount * order / leafs;
            var todo = (int)(reach - done);
            done = reach;
            long totalKeyLen = 0;
            for (var i = 0; i < todo; i++)
            {
                keys[i] = _transaction.GetKey().ToArray();
                totalKeyLen += keys[i].Length;
            }

            var newPrefixSize = TreeNodeUtils.CalcCommonPrefix(keys.AsSpan(0, todo));
            var newSuffixSize = totalKeyLen - todo * newPrefixSize;
            if (newSuffixSize >= ushort.MaxValue) throw new BTDBException("Keys of one leaf does not fit 64kb");
            var resOffset = (ulong)writer.GetCurrentPosition();
            if (resOffset > uint.MaxValue) throw new BTDBException("B+Tree does not fit into 4GB");
            writer.WriteUInt8((byte)todo);
            writer.WriteVUInt32(newPrefixSize);
            writer.WriteBlock(keys[0].AsSpan()[..(int)newPrefixSize]);
            var ofs = 0;
            for (var i = 0; i < todo; i++)
            {
                ofs += keys[i].Length - (int)newPrefixSize;
                writer.WriteUInt16LE((ushort)ofs);
            }

            for (var i = 0; i < todo; i++)
            {
                writer.WriteBlock(keys[i][(int)newPrefixSize..]);
            }

            for (var i = 0; i < todo; i++)
            {
                writer.WriteUInt32LE(valueOffsets[done - todo + i]);
            }

            return ((uint)resOffset, (uint)todo, keys[0]);
        }).Ptr;
    }

    delegate (uint Ptr, uint RecursiveChildren, byte[] FirstKey) BuildBranchNodeGenerator(ref SpanWriter writer);

    (uint Ptr, uint RecursiveChildren, byte[] FirstKey) BuildBranchNode(long count, ref SpanWriter writer,
        BuildBranchNodeGenerator generator)
    {
        if (count == 1) return generator(ref writer);
        var children = (count + _maxChildren - 1) / _maxChildren;
        var order = 0L;
        var done = 0L;
        var nodes = new uint[_maxChildren];
        var keys = new byte[_maxChildren][];
        return BuildBranchNode(children, ref writer, (ref SpanWriter writer) =>
        {
            order++;
            var reach = count * order / children;
            var todo = (int)(reach - done);
            done = reach;
            var totalSuffixLength = 0UL;
            var recursiveChildCount = 0u;
            for (var i = 0; i < todo; i++)
            {
                var child = generator(ref writer);
                nodes[i] = (uint)child.Ptr;
                recursiveChildCount += child.RecursiveChildren;
                keys[i] = child.FirstKey;
                if (i > 0) totalSuffixLength += (uint)keys[i].Length;
            }

            var newPrefixSize = TreeNodeUtils.CalcCommonPrefix(keys.AsSpan(0, todo));
            var newSuffixSize = totalSuffixLength - (uint)(todo - 1) * newPrefixSize;
            if (newSuffixSize >= ushort.MaxValue) throw new BTDBException("Keys of one leaf does not fit 64kb");
            var resOffset = (ulong)writer.GetCurrentPosition();
            if (resOffset > uint.MaxValue) throw new BTDBException("B+Tree does not fit into 4GB");
            writer.WriteUInt8((byte)(128+todo));
            writer.WriteVUInt32(recursiveChildCount);
            writer.WriteVUInt32(newPrefixSize);
            writer.WriteBlock(keys[0].AsSpan()[..(int)newPrefixSize]);
            var ofs = 0;
            for (var i = 1; i < todo; i++)
            {
                ofs += keys[i].Length - (int)newPrefixSize;
                writer.WriteUInt16LE((ushort)ofs);
            }

            for (var i = 1; i < todo; i++)
            {
                writer.WriteBlock(keys[i][(int)newPrefixSize..]);
            }

            for (var i = 0; i < todo; i++)
            {
                writer.WriteUInt32LE(nodes[i]);
            }

            return ((uint)resOffset, recursiveChildCount, keys[0]);
        });
    }

    void WriteHeader(ref SpanWriter writer)
    {
        writer.WriteBlock("roBTDB"u8);
        writer.WriteInt16(1);
        writer.WriteVUInt32((uint)_transaction.GetKeyValueCount());
        writer.WriteVUInt64(_transaction.GetCommitUlong());
        var ulongCount = _transaction.GetUlongCount();
        writer.WriteVUInt32(ulongCount);
        for (var i = 0u; i < ulongCount; i++)
        {
            writer.WriteVUInt64(_transaction.GetUlong(i));
        }
    }
}
