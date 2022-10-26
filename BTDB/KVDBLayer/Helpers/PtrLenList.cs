using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer;

public class PtrLenList
{
    public PtrLenList()
    {
        _list = null;
        _size = 0;
    }

    public bool Empty => _size == 0;

    public ulong FindFreeSizeAfter(ulong pos, ulong len)
    {
        if (len == 0) return pos;
        uint l = 0, r = _size;
        KeyValuePair<ulong, ulong> cur;
        while (l < r)
        {
            var m = (l + r) / 2;
            cur = _list![m];
            if (pos < cur.Key)
            {
                r = m;
            }
            else if (cur.Key + cur.Value <= pos)
            {
                l = m + 1;
            }
            else
            {
                pos = cur.Key + cur.Value;
                l = m + 1;
                break;
            }
        }
        while (l < _size)
        {
            cur = _list![l];
            if (pos + len <= cur.Key) return pos;
            pos = cur.Key + cur.Value;
            l++;
        }
        return pos;
    }

    public bool TryExclude(ulong excludePos, ulong excludeLen)
    {
        if (excludeLen == 0) return true;
        uint l = 0, r = _size;
        KeyValuePair<ulong, ulong> cur;
        while (l < r)
        {
            var m = (l + r) / 2;
            cur = _list![m];
            if (excludePos < cur.Key)
            {
                r = m;
            }
            else if (cur.Key + cur.Value <= excludePos)
            {
                l = m + 1;
            }
            else
            {
                if (excludePos == cur.Key)
                {
                    if (excludePos + excludeLen < cur.Key + cur.Value)
                    {
                        _list[m] = new(excludePos + excludeLen, cur.Value - excludeLen);
                        return true;
                    }
                    if (excludePos + excludeLen == cur.Key + cur.Value)
                    {
                        _size--;
                        Array.Copy(_list, m + 1, _list, m, _size - m);
                        return true;
                    }
                    r = m + 1;
                    while (r < _size && excludePos + excludeLen > _list[r].Key)
                    {
                        r++;
                    }
                    cur = _list[r - 1];
                    if (excludePos + excludeLen < cur.Key + cur.Value)
                    {
                        r--;
                        _list[r] = new(excludePos + excludeLen, cur.Key - excludePos + cur.Value - excludeLen);
                    }
                    Array.Copy(_list, r, _list, m, _size - r);
                    _size -= r - m;
                    return false;
                }
                if (excludePos + excludeLen == cur.Key + cur.Value)
                {
                    _list[m] = new(cur.Key, excludePos - cur.Key);
                    return true;
                }
                if (excludePos + excludeLen < cur.Key + cur.Value)
                {
                    GrowIfNeeded();
                    Array.Copy(_list, m + 1, _list, m + 2, _size - m - 1);
                    _size++;
                    _list[m] = new(cur.Key, excludePos - cur.Key);
                    _list[m + 1] = new(excludePos + excludeLen, cur.Key + cur.Value - excludePos - excludeLen);
                    return true;
                }
                _list[m] = new(cur.Key, excludePos - cur.Key);
                l = m + 1;
                break;
            }
        }
        if (l == _size)
        {
            return false;
        }
        r = l + 1;
        while (r < _size && excludePos + excludeLen > _list![r].Key)
        {
            r++;
        }
        cur = _list![r - 1];
        if (excludePos + excludeLen < cur.Key + cur.Value)
        {
            r--;
            _list[r] = new(excludePos + excludeLen, cur.Key - excludePos + cur.Value - excludeLen);
        }
        Array.Copy(_list, r, _list, l, _size - r);
        _size -= r - l;
        return false;
    }

    public bool TryFindLenAndRemove(ulong findLength, out ulong foundOnPosition)
    {
        for (var i = 0; i < _size; i++)
        {
            var len = _list![i].Value;
            if (findLength > len) continue;
            foundOnPosition = _list[i].Key;
            if (findLength == len)
            {
                _size--;
                Array.Copy(_list, i + 1, _list, i, _size - i);
            }
            else
            {
                _list[i] = new(foundOnPosition + findLength, len - findLength);
            }
            return true;
        }
        foundOnPosition = 0;
        return false;
    }

    public bool TryInclude(ulong includePos, ulong includeLen)
    {
        if (includeLen == 0) return true;
        if (_list == null)
        {
            _list = new KeyValuePair<ulong, ulong>[4];
            _size = 1;
            _list[0] = new(includePos, includeLen);
            return true;
        }
        uint l = 0, r = _size;
        KeyValuePair<ulong, ulong> cur;
        while (l < r)
        {
            var m = (l + r) / 2;
            cur = _list[m];
            if (includePos < cur.Key)
            {
                r = m;
            }
            else if (cur.Key + cur.Value < includePos)
            {
                l = m + 1;
            }
            else
            {
                if (includePos + includeLen <= cur.Key + cur.Value)
                {
                    return false;
                }
                var result = true;
                l = m + 1;
                if (includePos < cur.Key + cur.Value) result = false;
                else
                {
                    if (l < _size && includePos + includeLen > _list[l].Key)
                    {
                        l++;
                        result = false;
                    }
                }
                while (l < _size && includePos + includeLen >= _list[l].Key)
                {
                    l++;
                }
                var lastOk = _list[l - 1];
                _list[m] = new(cur.Key, Math.Max(lastOk.Key + lastOk.Value, includePos + includeLen) - cur.Key);
                Array.Copy(_list, l, _list, m + 1, _size - l);
                _size -= l - (m + 1);
                return result;
            }
        }
        if (l == _size)
        {
            GrowIfNeeded();
            _list[l] = new(includePos, includeLen);
            _size++;
            return true;
        }
        cur = _list[l];
        if (includePos + includeLen < cur.Key)
        {
            GrowIfNeeded();
            Array.Copy(_list, l, _list, l + 1, _size - l);
            _list[l] = new(includePos, includeLen);
            _size++;
            return true;
        }
        if (includePos + includeLen == cur.Key)
        {
            _list[l] = new(includePos, cur.Key + cur.Value - includePos);
            return true;
        }
        while (r < _size && includePos + includeLen >= _list[r].Key)
        {
            r++;
        }
        cur = _list[r - 1];
        _list[l] = new(includePos, Math.Max(cur.Key + cur.Value, includePos + includeLen) - includePos);
        Array.Copy(_list, r, _list, l + 1, _size - r);
        _size -= r - (l + 1);
        return false;
    }

    public PtrLenList MergeIntoNew(PtrLenList? mergeWith)
    {
        if (mergeWith == null || mergeWith.Empty) return Clone();
        if (Empty) return mergeWith.Clone();

        // TODO: optimize this
        var result = Clone();
        foreach (var range in mergeWith)
        {
            result.TryInclude(range.Key, range.Value);
        }
        return result;
    }

    public void Clear()
    {
        _size = 0;
    }

    public IEnumerator<KeyValuePair<ulong, ulong>> GetEnumerator()
    {
        for (var i = 0; i < _size; i++)
        {
            yield return _list![i];
        }
    }

    public PtrLenList CloneAndClear()
    {
        var result = Clone();
        Clear();
        return result;
    }

    public PtrLenList Clone()
    {
        var res = new PtrLenList();
        if (_size == 0) return res;
        res._size = _size;
        res._list = new KeyValuePair<ulong, ulong>[_size];
        Array.Copy(_list!, res._list, _size);
        return res;
    }

    public void MergeInPlace(PtrLenList? mergeWith)
    {
        if (mergeWith == null) return;
        foreach (var range in mergeWith)
        {
            TryInclude(range.Key, range.Value);
        }
    }

    internal void UnmergeInPlace(PtrLenList? unmergeWith)
    {
        if (unmergeWith == null) return;
        foreach (var range in unmergeWith)
        {
            TryExclude(range.Key, range.Value);
        }
    }

    public bool Contains(ulong position)
    {
        uint l = 0, r = _size;
        while (l < r)
        {
            var m = (l + r) / 2;
            var cur = _list![m];
            if (position < cur.Key)
            {
                r = m;
            }
            else if (cur.Key + cur.Value <= position)
            {
                l = m + 1;
            }
            else
            {
                return true;
            }
        }
        return false;
    }

    void GrowIfNeeded()
    {
        if (_size == _list!.Length)
        {
            Array.Resize(ref _list, (int)_size * 2);
        }
    }

    KeyValuePair<ulong, ulong>[]? _list;
    uint _size;
}
