using System.Collections.Generic;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public class InMemoryFileCollection : IFileCollection
    {
        readonly List<IPositionLessStream> _list = new List<IPositionLessStream>();

        public int AddFile()
        {
            lock (_list)
            {
                _list.Add(new MemoryPositionLessStream());
                return _list.Count - 1;
            }
        }

        public int GetCount()
        {
            lock (_list)
            {
                return _list.Count;
            }
        }

        public IPositionLessStream GetFile(int index)
        {
            lock (_list)
            {
                return _list[index];
            }
        }

        public void RemoveFile(int index)
        {
            lock (_list)
            {
                _list[index] = null;
            }
        }
    }
}
