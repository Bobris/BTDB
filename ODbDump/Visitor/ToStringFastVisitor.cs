using System.Text;

namespace ODbDump.Visitor
{
    internal class ToStringFastVisitor : ToConsoleVisitorNice
    {
        protected readonly StringBuilder Builder = new StringBuilder();

        public override string ToString()
        {
            return Builder.ToString();
        }

        public override void Print(string s)
        {
            Builder.Append(s);
        }
    }
}