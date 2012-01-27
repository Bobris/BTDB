using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public interface IFileCollection
    {
        int AddFile();
        int GetCount();
        IPositionLessStream GetFile(int index);
        void RemoveFile(int index);
    }
}