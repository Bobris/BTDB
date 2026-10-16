using System;
using BTDB.LSM.Format;

namespace BTDB.LSM.Engine;

/// A positioned cursor over one component's points (live and deleted), moving in both directions.
interface IComponentCursor
{
    bool Valid { get; }
    LsmEntry Current { get; }
    bool SeekGE(ReadOnlySpan<byte> key, bool inclusive);
    bool SeekLE(ReadOnlySpan<byte> key, bool inclusive);
    bool SeekLast();
    bool Next();
    bool Prev();
}

/// Component cursor that repositions by seeking; used where seeks are as cheap as steps (the memtable).
sealed class SeekingComponentCursor(ILsmComponent component) : IComponentCursor
{
    LsmEntry _current;

    public bool Valid { get; private set; }
    public LsmEntry Current => _current;

    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive) => Valid = component.SeekGE(key, inclusive, out _current);
    public bool SeekLE(ReadOnlySpan<byte> key, bool inclusive) => Valid = component.SeekLE(key, inclusive, out _current);
    public bool SeekLast() => Valid = component.SeekLast(out _current);
    public bool Next() => Valid = component.SeekGE(_current.Key, false, out _current);
    public bool Prev() => Valid = component.SeekLE(_current.Key, false, out _current);
}

/// Positioned merging iterator over a view's components (ImplementationPlan.md, section 7.3). Each child cursor stays
/// positioned; a step advances only the children at the current key, and a fragment masking older components moves
/// their children past it in one seek. Changing direction reseeks every child from the current key.
sealed class MergingIterator
{
    readonly LsmView _view;
    readonly IComponentCursor[] _children;
    int _direction; // 1 forward, -1 backward, 0 unpositioned
    byte[] _key = [];
    LsmValue _value;

    public MergingIterator(LsmView view)
    {
        _view = view;
        var components = view.Components;
        _children = new IComponentCursor[components.Length];
        for (var i = 0; i < components.Length; i++) _children[i] = components[i].CreateCursor();
    }

    public LsmView View => _view;
    public bool Valid => _direction != 0;
    public byte[] Key => _key;
    public LsmValue Value => _value;

    public void Invalidate() => _direction = 0;

    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive)
    {
        _view.Prefetch(key);
        foreach (var child in _children) child.SeekGE(key, inclusive);
        return ResolveForward();
    }

    /// Last live key lower than (or equal to, when inclusive) key; unbounded seeks the last live key.
    public bool SeekLE(ReadOnlySpan<byte> key, bool unbounded, bool inclusive)
    {
        if (!unbounded) _view.Prefetch(key);
        foreach (var child in _children)
        {
            if (unbounded) child.SeekLast();
            else child.SeekLE(key, inclusive);
        }

        return ResolveBackward();
    }

    public bool Next()
    {
        if (_direction != 1) return _direction != 0 && SeekGE(_key, false);
        foreach (var child in _children)
        {
            if (child.Valid && LsmKeys.Compare(child.Current.Key, _key) == 0) child.Next();
        }

        return ResolveForward();
    }

    public bool Prev()
    {
        if (_direction != -1) return _direction != 0 && SeekLE(_key, false, false);
        foreach (var child in _children)
        {
            if (child.Valid && LsmKeys.Compare(child.Current.Key, _key) == 0) child.Prev();
        }

        return ResolveBackward();
    }

    bool ResolveForward()
    {
        var components = _view.Components;
        while (true)
        {
            byte[]? best = null;
            foreach (var child in _children)
            {
                if (child.Valid && (best == null || LsmKeys.Compare(child.Current.Key, best) < 0))
                    best = child.Current.Key;
            }

            if (best == null) return Unpositioned();
            var masked = -1;
            for (var i = 0; i < _children.Length; i++)
            {
                var child = _children[i];
                if (child.Valid && LsmKeys.Compare(child.Current.Key, best) == 0)
                {
                    if (child.Current.IsSet) return Positioned(1, best, child.Current.LsmValue);
                    break;
                }

                if (components[i].Tombstones.Find(best, out _) is >= 0 and var fragment)
                {
                    masked = i;
                    // Everything older than the masking component is hidden up to the fragment end.
                    var end = components[i].Tombstones[fragment].End;
                    for (var j = i + 1; j < _children.Length; j++)
                    {
                        if (_children[j].Valid && LsmKeys.Compare(_children[j].Current.Key, end) < 0)
                            _children[j].SeekGE(end, true);
                    }

                    break;
                }
            }

            for (var i = 0; i < (masked >= 0 ? masked + 1 : _children.Length); i++)
            {
                var child = _children[i];
                if (child.Valid && LsmKeys.Compare(child.Current.Key, best) == 0) child.Next();
            }
        }
    }

    bool ResolveBackward()
    {
        var components = _view.Components;
        while (true)
        {
            byte[]? best = null;
            foreach (var child in _children)
            {
                if (child.Valid && (best == null || LsmKeys.Compare(child.Current.Key, best) > 0))
                    best = child.Current.Key;
            }

            if (best == null) return Unpositioned();
            var masked = -1;
            for (var i = 0; i < _children.Length; i++)
            {
                var child = _children[i];
                if (child.Valid && LsmKeys.Compare(child.Current.Key, best) == 0)
                {
                    if (child.Current.IsSet) return Positioned(-1, best, child.Current.LsmValue);
                    break;
                }

                if (components[i].Tombstones.Find(best, out _) is >= 0 and var fragment)
                {
                    masked = i;
                    var start = components[i].Tombstones[fragment].Start;
                    for (var j = i + 1; j < _children.Length; j++)
                    {
                        if (_children[j].Valid && LsmKeys.Compare(_children[j].Current.Key, start) >= 0)
                            _children[j].SeekLE(start, false);
                    }

                    break;
                }
            }

            for (var i = 0; i < (masked >= 0 ? masked + 1 : _children.Length); i++)
            {
                var child = _children[i];
                if (child.Valid && LsmKeys.Compare(child.Current.Key, best) == 0) child.Prev();
            }
        }
    }

    bool Positioned(int direction, byte[] key, LsmValue value)
    {
        _direction = direction;
        _key = key;
        _value = value;
        return true;
    }

    bool Unpositioned()
    {
        _direction = 0;
        return false;
    }
}
