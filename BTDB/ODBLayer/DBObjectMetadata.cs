namespace BTDB.ODBLayer
{
    public class DBObjectMetadata
    {
        readonly ulong _id;
        bool _dirty;

        public DBObjectMetadata(ulong id, bool dirty)
        {
            _id = id;
            _dirty = dirty;
        }

        public bool Dirty
        {
            get { return _dirty; }
            set { _dirty = value; }
        }

        public ulong Id
        {
            get { return _id; }
        }
    }
}