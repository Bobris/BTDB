using System.IO;

namespace ODbDump.Visitor
{
    class ToFileVisitorNice : ToConsoleVisitorNice
    {
        readonly StreamWriter _streamWriter;

        public ToFileVisitorNice(StreamWriter streamWriter)
        {
            _streamWriter = streamWriter;
        }

        public override void Print(string s)
        {
            _streamWriter.WriteLine(new string(' ', _indent * 2) + s);
        }
    }
}