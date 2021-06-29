namespace ODbDump.Visitor
{
    class ToFilesVisitorWithSecondaryKeys : ToFilesVisitorForComparison
    {
        bool _insideSecKey;

        public ToFilesVisitorWithSecondaryKeys() : base(HashType.None)
        {
        }

        public override void Print(string s)
        {
            if (_insideSecKey)
            {
                Output!.Write(new string(' ', _indent * 2 + 2) + s);
            }
            else
            {
                base.Print(s);
            }
        }

        public override bool StartSecondaryIndex(string name)
        {
            Print($"SK:[{name}]");
            _insideSecKey = true;
            return true;
        }

        public override void NextSecondaryKey()
        {
            Output!.WriteLine();
        }

        public override void EndSecondaryIndex()
        {
            _insideSecKey = false;
        }
    }
}