using System;
using System.Collections.Generic;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public interface IFileCollection : IDisposable
    {
        int AddFile(string humanHint);
        int GetCount();
        IPositionLessStream GetFile(int index);
        void RemoveFile(int index);
        IEnumerable<int> Enumerate();
    }
}