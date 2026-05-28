using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;

namespace BTDB.ODBLayer;

public interface IRoaringBitmapOp
{
}

public static class RoaringBitmap
{
    const byte CommandClear = 0;
    const byte CommandPage = 1;
    const byte CommandCount = 2;
    static readonly byte[] FullPageContent = [0, 0, 255, 255, 0];

    public static IRoaringBitmapOp Source(IRoaringBitmap bitmap)
    {
        if (bitmap is IInternalODBRoaringBitmap internalBitmap)
            return new SourceRoaringBitmapOp(internalBitmap);
        return new SourceEnumerableOp(bitmap);
    }

    public static IRoaringBitmapOp Source(ulong[] values)
    {
        return new SourceEnumerableOp(values);
    }

    public static IRoaringBitmapOp Source(IEnumerable<ulong> values)
    {
        return new SourceEnumerableOp(values);
    }

    public static IRoaringBitmapOp Op(IRoaringBitmapOp left, IRoaringBitmapOp right)
    {
        return Or(left, right);
    }

    public static IRoaringBitmapOp Or(IRoaringBitmapOp left, IRoaringBitmapOp right)
    {
        return new BinaryOp(BinaryOperation.Or, ToOp(left), ToOp(right));
    }

    public static IRoaringBitmapOp And(IRoaringBitmapOp left, IRoaringBitmapOp right)
    {
        return new BinaryOp(BinaryOperation.And, ToOp(left), ToOp(right));
    }

    public static IRoaringBitmapOp Not(IRoaringBitmapOp operation)
    {
        var op = ToOp(operation);
        return op is NotOp not ? not.Inner : new NotOp(op);
    }

    public static async Task BuildAsync(IRoaringBitmapOp operation,
        Func<ReadOnlyMemory<byte>, CancellationToken, Task> applier, Memory<byte> buffer,
        ulong maxValidIndex, CancellationToken cancellation = default)
    {
        if (buffer.Length < RoaringBitmaps.BitmapSize + 32)
            throw new ArgumentException("Buffer must have at least RoaringBitmaps.BitmapSize + 32 bytes.",
                nameof(buffer));
        var writer = new CommandWriter(buffer, applier, cancellation);
        var op = ToOp(operation);
        writer.WriteClear();

        using var reader = op.Pages(maxValidIndex);
        while (reader.ReadNext() is { } pageIndex)
        {
            cancellation.ThrowIfCancellationRequested();
            var pageContent = writer.StartPage(pageIndex);
            var pageValue = reader.ReadValue(pageContent);
            await writer.CommitPage((uint)pageValue.Length).ConfigureAwait(false);
        }

        writer.WriteCount(writer.Count);
        await writer.FlushAsync().ConfigureAwait(false);
    }

    static RoaringBitmapOpBase ToOp(IRoaringBitmapOp operation)
    {
        return operation as RoaringBitmapOpBase ??
               throw new ArgumentException("Unknown IRoaringBitmapOp implementation.", nameof(operation));
    }

    static ulong Count(ReadOnlyMemory<byte> encoded)
    {
        var result = 0ul;
        foreach (var _ in RoaringBitmaps.Enumerate(encoded, 0))
            result++;
        return result;
    }

    abstract class RoaringBitmapOpBase : IRoaringBitmapOp
    {
        internal abstract IInternalODBRoaringBitmapPageReader Pages(ulong maxValidIndex);
    }

    sealed class NotOp : RoaringBitmapOpBase
    {
        internal readonly RoaringBitmapOpBase Inner;

        public NotOp(RoaringBitmapOpBase inner)
        {
            Inner = inner;
        }

        internal override IInternalODBRoaringBitmapPageReader Pages(ulong maxValidIndex)
        {
            return new NotPageReader(Inner.Pages(maxValidIndex), maxValidIndex);
        }

    }

    sealed class SourceEnumerableOp : RoaringBitmapOpBase
    {
        readonly IEnumerable<ulong> _values;

        public SourceEnumerableOp(IEnumerable<ulong> values)
        {
            _values = values;
        }

        internal override IInternalODBRoaringBitmapPageReader Pages(ulong maxValidIndex)
        {
            return new SourceEnumerablePageReader(_values, maxValidIndex);
        }

    }

    sealed class SourceRoaringBitmapOp : RoaringBitmapOpBase
    {
        readonly IInternalODBRoaringBitmap _bitmap;

        public SourceRoaringBitmapOp(IInternalODBRoaringBitmap bitmap)
        {
            _bitmap = bitmap;
        }

        internal override IInternalODBRoaringBitmapPageReader Pages(ulong maxValidIndex)
        {
            return new BoundedPageReader(_bitmap.GetPageReader(), maxValidIndex);
        }

    }

    enum BinaryOperation
    {
        Or,
        And
    }

    sealed class BinaryOp : RoaringBitmapOpBase
    {
        readonly BinaryOperation _operation;
        readonly RoaringBitmapOpBase _left;
        readonly RoaringBitmapOpBase _right;

        public BinaryOp(BinaryOperation operation, RoaringBitmapOpBase left, RoaringBitmapOpBase right)
        {
            _operation = operation;
            _left = left;
            _right = right;
        }

        internal override IInternalODBRoaringBitmapPageReader Pages(ulong maxValidIndex)
        {
            if (_operation == BinaryOperation.And && _right is NotOp notRight)
                return new AndNotPageReader(_left.Pages(maxValidIndex), notRight.Inner.Pages(maxValidIndex),
                    maxValidIndex);
            if (_operation == BinaryOperation.And && _left is NotOp notLeft)
                return new AndNotPageReader(_right.Pages(maxValidIndex), notLeft.Inner.Pages(maxValidIndex),
                    maxValidIndex);
            return new BinaryPageReader(_operation, _left.Pages(maxValidIndex), _right.Pages(maxValidIndex));
        }

    }

    sealed class SourceEnumerablePageReader : IInternalODBRoaringBitmapPageReader
    {
        readonly IEnumerator<ulong> _values;
        readonly ulong _maxValidIndex;
        readonly byte[] _bitmap = new byte[RoaringBitmaps.BitmapSize];
        readonly byte[] _content = new byte[RoaringBitmaps.BitmapSize];
        ulong? _nextValue;
        int _contentLength;
        bool _finished;

        public SourceEnumerablePageReader(IEnumerable<ulong> values, ulong maxValidIndex)
        {
            _values = values.GetEnumerator();
            _maxValidIndex = maxValidIndex;
        }

        public ulong? ReadNext()
        {
            if (_finished)
                return null;
            _bitmap.AsSpan().Clear();

            ulong value;
            if (_nextValue.HasValue)
            {
                value = _nextValue.Value;
                _nextValue = null;
            }
            else if (!TryReadValue(out value))
            {
                _finished = true;
                return null;
            }

            var pageIndex = value >> 16;
            while (true)
            {
                RoaringBitmaps.SetBit(_bitmap, (ushort)value);
                if (!TryReadValue(out value))
                {
                    _finished = true;
                    break;
                }

                if (value >> 16 != pageIndex)
                {
                    _nextValue = value;
                    break;
                }
            }

            _contentLength = RoaringBitmaps.Compress(_bitmap, _content);
            return pageIndex;
        }

        public Memory<byte> ReadValue(Memory<byte> content)
        {
            _content.AsMemory(0, _contentLength).CopyTo(content);
            return content[.._contentLength];
        }

        bool TryReadValue(out ulong value)
        {
            if (_values.MoveNext() && _values.Current <= _maxValidIndex)
            {
                value = _values.Current;
                return true;
            }

            value = 0;
            return false;
        }

        public void Dispose()
        {
            _values.Dispose();
        }
    }

    sealed class BoundedPageReader : IInternalODBRoaringBitmapPageReader
    {
        readonly IInternalODBRoaringBitmapPageReader _inner;
        readonly ulong _maxValidIndex;
        readonly ulong _maxPage;
        ulong _pageIndex;
        bool _finished;

        public BoundedPageReader(IInternalODBRoaringBitmapPageReader inner, ulong maxValidIndex)
        {
            _inner = inner;
            _maxValidIndex = maxValidIndex;
            _maxPage = maxValidIndex >> 16;
        }

        public ulong? ReadNext()
        {
            if (_finished)
                return null;
            var pageIndex = _inner.ReadNext();
            if (!pageIndex.HasValue || pageIndex.Value > _maxPage)
            {
                _finished = true;
                return null;
            }

            _pageIndex = pageIndex.Value;
            return _pageIndex;
        }

        public Memory<byte> ReadValue(Memory<byte> content)
        {
            var value = _inner.ReadValue(content);
            if (_pageIndex == _maxPage)
            {
                var len = RoaringBitmaps.RemoveValuesGreaterThanInPlace(value.Span, (ushort)_maxValidIndex);
                return value[..len];
            }

            return value;
        }

        public void Dispose()
        {
            _inner.Dispose();
        }
    }

    sealed class NotPageReader : IInternalODBRoaringBitmapPageReader
    {
        readonly IInternalODBRoaringBitmapPageReader _source;
        readonly ulong _maxValidIndex;
        readonly ulong _maxPage;
        readonly byte[] _sourceBuffer = new byte[RoaringBitmaps.BitmapSize];
        ulong? _sourcePageIndex;
        ulong _pageIndex;
        ulong _currentPageIndex;
        bool _initialized;
        bool _finished;

        public NotPageReader(IInternalODBRoaringBitmapPageReader source, ulong maxValidIndex)
        {
            _source = source;
            _maxValidIndex = maxValidIndex;
            _maxPage = maxValidIndex >> 16;
        }

        public ulong? ReadNext()
        {
            if (_finished)
                return null;
            if (!_initialized)
            {
                _sourcePageIndex = _source.ReadNext();
                _initialized = true;
            }

            _currentPageIndex = _pageIndex;
            if (_pageIndex == _maxPage)
                _finished = true;
            else
                _pageIndex++;
            return _currentPageIndex;
        }

        public Memory<byte> ReadValue(Memory<byte> content)
        {
            while (_sourcePageIndex.HasValue && _sourcePageIndex.Value < _currentPageIndex)
                _sourcePageIndex = _source.ReadNext();
            int len;
            if (_sourcePageIndex.HasValue && _sourcePageIndex.Value == _currentPageIndex)
            {
                len = RoaringBitmaps.Not(_source.ReadValue(_sourceBuffer).Span, content.Span);
                _sourcePageIndex = _source.ReadNext();
            }
            else
            {
                len = Copy(FullPage().Span, content.Span);
            }

            if (_currentPageIndex == _maxPage)
                len = RoaringBitmaps.RemoveValuesGreaterThanInPlace(content.Span[..len], (ushort)_maxValidIndex);
            return content[..len];
        }

        public void Dispose()
        {
            _source.Dispose();
        }
    }

    sealed class AndNotPageReader : IInternalODBRoaringBitmapPageReader
    {
        readonly IInternalODBRoaringBitmapPageReader _left;
        readonly IInternalODBRoaringBitmapPageReader _right;
        readonly ulong _maxValidIndex;
        readonly ulong _maxPage;
        readonly byte[] _leftBuffer = new byte[RoaringBitmaps.BitmapSize];
        readonly byte[] _rightBuffer = new byte[RoaringBitmaps.BitmapSize];
        ulong? _leftPageIndex;
        ulong? _rightPageIndex;
        ulong _currentPageIndex;
        bool _initialized;

        public AndNotPageReader(IInternalODBRoaringBitmapPageReader left, IInternalODBRoaringBitmapPageReader right,
            ulong maxValidIndex)
        {
            _left = left;
            _right = right;
            _maxValidIndex = maxValidIndex;
            _maxPage = maxValidIndex >> 16;
        }

        public ulong? ReadNext()
        {
            Initialize();
            if (!_leftPageIndex.HasValue)
                return null;
            _currentPageIndex = _leftPageIndex.Value;
            return _currentPageIndex;
        }

        public Memory<byte> ReadValue(Memory<byte> content)
        {
            var leftContent = _left.ReadValue(_leftBuffer);
            while (_rightPageIndex.HasValue && _rightPageIndex.Value < _currentPageIndex)
                _rightPageIndex = _right.ReadNext();
            int len;
            if (_rightPageIndex.HasValue && _rightPageIndex.Value == _currentPageIndex)
            {
                len = RoaringBitmaps.AndNot(leftContent.Span, _right.ReadValue(_rightBuffer).Span, content.Span);
                _rightPageIndex = _right.ReadNext();
            }
            else
            {
                leftContent.CopyTo(content);
                len = leftContent.Length;
            }

            if (_currentPageIndex == _maxPage)
                len = RoaringBitmaps.RemoveValuesGreaterThanInPlace(content.Span[..len], (ushort)_maxValidIndex);
            _leftPageIndex = _left.ReadNext();
            return content[..len];
        }

        void Initialize()
        {
            if (_initialized)
                return;
            _leftPageIndex = _left.ReadNext();
            _rightPageIndex = _right.ReadNext();
            _initialized = true;
        }

        public void Dispose()
        {
            _left.Dispose();
            _right.Dispose();
        }
    }

    sealed class BinaryPageReader : IInternalODBRoaringBitmapPageReader
    {
        enum CurrentPageSource
        {
            Left,
            Right,
            Both
        }

        readonly BinaryOperation _operation;
        readonly IInternalODBRoaringBitmapPageReader _left;
        readonly IInternalODBRoaringBitmapPageReader _right;
        readonly byte[] _leftBuffer = new byte[RoaringBitmaps.BitmapSize];
        readonly byte[] _rightBuffer = new byte[RoaringBitmaps.BitmapSize];
        ulong? _leftPageIndex;
        ulong? _rightPageIndex;
        ulong _currentPageIndex;
        CurrentPageSource _currentPageSource;
        bool _initialized;

        public BinaryPageReader(BinaryOperation operation, IInternalODBRoaringBitmapPageReader left,
            IInternalODBRoaringBitmapPageReader right)
        {
            _operation = operation;
            _left = left;
            _right = right;
        }

        public ulong? ReadNext()
        {
            Initialize();
            if (_operation == BinaryOperation.And)
                return ReadNextAnd();
            return ReadNextOr();
        }

        ulong? ReadNextAnd()
        {
            while (_leftPageIndex.HasValue && _rightPageIndex.HasValue)
            {
                if (_leftPageIndex.Value < _rightPageIndex.Value)
                {
                    _leftPageIndex = _left.ReadNext();
                    continue;
                }

                if (_rightPageIndex.Value < _leftPageIndex.Value)
                {
                    _rightPageIndex = _right.ReadNext();
                    continue;
                }

                _currentPageIndex = _leftPageIndex.Value;
                _currentPageSource = CurrentPageSource.Both;
                return _currentPageIndex;
            }

            return null;
        }

        ulong? ReadNextOr()
        {
            if (!_leftPageIndex.HasValue && !_rightPageIndex.HasValue)
                return null;
            if (!_rightPageIndex.HasValue || _leftPageIndex.HasValue && _leftPageIndex.Value < _rightPageIndex.Value)
            {
                _currentPageIndex = _leftPageIndex!.Value;
                _currentPageSource = CurrentPageSource.Left;
                return _currentPageIndex;
            }

            if (!_leftPageIndex.HasValue || _rightPageIndex.Value < _leftPageIndex.Value)
            {
                _currentPageIndex = _rightPageIndex.Value;
                _currentPageSource = CurrentPageSource.Right;
                return _currentPageIndex;
            }

            _currentPageIndex = _leftPageIndex.Value;
            _currentPageSource = CurrentPageSource.Both;
            return _currentPageIndex;
        }

        public Memory<byte> ReadValue(Memory<byte> content)
        {
            int len;
            if (_currentPageSource == CurrentPageSource.Left)
            {
                var leftContent = _left.ReadValue(_leftBuffer);
                leftContent.CopyTo(content);
                len = leftContent.Length;
                _leftPageIndex = _left.ReadNext();
                return content[..len];
            }

            if (_currentPageSource == CurrentPageSource.Right)
            {
                var rightContent = _right.ReadValue(_rightBuffer);
                rightContent.CopyTo(content);
                len = rightContent.Length;
                _rightPageIndex = _right.ReadNext();
                return content[..len];
            }

            len = _operation == BinaryOperation.And
                ? RoaringBitmaps.And(_left.ReadValue(_leftBuffer).Span, _right.ReadValue(_rightBuffer).Span,
                    content.Span)
                : RoaringBitmaps.Or(_left.ReadValue(_leftBuffer).Span, _right.ReadValue(_rightBuffer).Span,
                    content.Span);
            _leftPageIndex = _left.ReadNext();
            _rightPageIndex = _right.ReadNext();
            return content[..len];
        }

        void Initialize()
        {
            if (_initialized)
                return;
            _leftPageIndex = _left.ReadNext();
            _rightPageIndex = _right.ReadNext();
            _initialized = true;
        }

        public void Dispose()
        {
            _left.Dispose();
            _right.Dispose();
        }
    }

    static int Copy(ReadOnlySpan<byte> source, Span<byte> target)
    {
        source.CopyTo(target);
        return source.Length;
    }

    static ReadOnlyMemory<byte> FullPage()
    {
        return FullPageContent;
    }

    sealed class CommandWriter
    {
        const int PageLengthBytes = sizeof(ushort);
        static readonly int MaxPageCommandLength = AlignToUshort(1 + 9) + PageLengthBytes + RoaringBitmaps.BitmapSize;

        readonly Memory<byte> _buffer;
        readonly Func<ReadOnlyMemory<byte>, CancellationToken, Task> _applier;
        readonly CancellationToken _cancellation;
        int _pos;
        int _pageCommandStart;
        int _pageLengthPosition;
        int _pageContentStart;

        public ulong Count { get; private set; }

        public CommandWriter(Memory<byte> buffer, Func<ReadOnlyMemory<byte>, CancellationToken, Task> applier,
            CancellationToken cancellation)
        {
            _buffer = buffer;
            _applier = applier;
            _cancellation = cancellation;
        }

        public void WriteClear()
        {
            Count = 0;
            _buffer.Span[_pos++] = CommandClear;
        }

        public Memory<byte> StartPage(ulong pageIndex)
        {
            if (_buffer.Length < MaxPageCommandLength)
                throw new ArgumentException("Buffer is too small for one roaring bitmap command.");
            if (_buffer.Length - _pos < MaxPageCommandLength)
                FlushAsync().AsTask().GetAwaiter().GetResult();
            _pageCommandStart = _pos;
            var span = _buffer.Span;
            span[_pos++] = CommandPage;
            WriteVUInt(pageIndex);
            _pos = AlignToUshort(_pos);
            _pageLengthPosition = _pos;
            _pos += PageLengthBytes;
            _pageContentStart = _pos;
            return _buffer.Slice(_pageContentStart, RoaringBitmaps.BitmapSize);
        }

        public async ValueTask CommitPage(uint byteLength)
        {
            if (byteLength > RoaringBitmaps.BitmapSize)
                throw new ArgumentException("Page payload is too long.", nameof(byteLength));
            if (byteLength == 0)
            {
                _pos = _pageCommandStart;
                return;
            }

            BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Span.Slice(_pageLengthPosition, PageLengthBytes),
                (ushort)byteLength);
            _pos = _pageContentStart + (int)byteLength;
            Count += RoaringBitmap.Count(_buffer.Slice(_pageContentStart, (int)byteLength));
            if (_buffer.Length - _pos < MaxPageCommandLength)
                await FlushAsync().ConfigureAwait(false);
        }

        public void WriteCount(ulong count)
        {
            _buffer.Span[_pos++] = CommandCount;
            WriteVUInt(count);
        }

        public async ValueTask FlushAsync()
        {
            if (_pos == 0)
                return;
            await _applier(_buffer[.._pos], _cancellation).ConfigureAwait(false);
            _pos = 0;
        }

        void WriteVUInt(ulong value)
        {
            var len = PackUnpack.LengthVUInt(value);
            PackUnpack.UnsafePackVUInt(ref _buffer.Span[_pos], value, len);
            _pos += (int)len;
        }

        static int AlignToUshort(int offset)
        {
            return (offset + 1) & ~1;
        }
    }
}
