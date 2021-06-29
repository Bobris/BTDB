using System;
using System.Text;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace ODbDump.Visitor
{
    class ToConsoleFastVisitor : IODBFastVisitor
    {
        internal int _indent = 0;

        readonly StringBuilder _builder = new StringBuilder();

        public void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr)
        {
        }

        public virtual void Print(string s)
        {
            Console.WriteLine(new string(' ', _indent * 2) + s);
        }
    }
}