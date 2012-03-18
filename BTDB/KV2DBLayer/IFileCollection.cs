using System;
using System.Collections.Generic;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public interface IFileCollection : IDisposable
    {
        uint AddFile(string humanHint);
        uint GetCount();
        IPositionLessStream GetFile(uint index);
        void RemoveFile(uint index);
        IEnumerable<uint> Enumerate();
    }
}