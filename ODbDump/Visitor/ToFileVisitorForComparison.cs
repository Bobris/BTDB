using System;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using BTDB.ODBLayer;
using Force.Crc32;

namespace ODbDump.Visitor
{
    enum HashType
    {
        None,
        Sha256,
        Crc32
    }

    class ToFilesVisitorForComparison : ToConsoleVisitorForComparison, IDisposable
    {
        readonly bool _hashStrings;
        readonly HashAlgorithm? _hashAlgorithm;
        StreamWriter? _output;
        readonly string _fileSuffix;

        public ToFilesVisitorForComparison(HashType hashType)
        {
            _hashStrings = hashType != HashType.None;
            switch (hashType)
            {
                case HashType.None:
                    _fileSuffix = ".txt";
                    break;
                case HashType.Sha256:
                    (_hashAlgorithm, _fileSuffix) = (SHA256.Create(), "-SHA256.txt");
                    break;
                case HashType.Crc32:
                    (_hashAlgorithm, _fileSuffix) = (new Crc32Algorithm(), "-CRC32.txt");
                    break;
                default:
                    throw new NotSupportedException();
            }
        }

        StreamWriter OpenOutputStream(string filename) =>
            new StreamWriter(File.Open(filename, FileMode.Create, FileAccess.Write));

        string ToValidFilename(string filename) =>
            string.Join("_", filename.Split(Path.GetInvalidFileNameChars())) + _fileSuffix;

        public override void Print(string s)
        {
            _output!.WriteLine(new string(' ', _indent * 2) + s);
        }

        public override bool NeedScalarAsText()
        {
            return false;
        }

        public override bool NeedScalarAsObject()
        {
            return true;
        }

        public override void ScalarAsObject(object? content)
        {
            if (_hashStrings && content is string str)
            {
                var hash = _hashAlgorithm!.ComputeHash(Encoding.UTF8.GetBytes(str));
                var sb = new StringBuilder();
                foreach (var @byte in hash)
                    sb.Append(@byte.ToString("X2"));

                ScalarAsText(sb.ToString());
            }
            else
            {
                ScalarAsText(string.Format(CultureInfo.InvariantCulture, "{0}", content));
            }
        }

        public override bool VisitSingleton(uint tableId, string? tableName, ulong oid)
        {
            _output?.Dispose();
            _output = OpenOutputStream($"{ToValidFilename(tableName ?? $"Unknown_{tableId}_{oid}")}.txt");

            return base.VisitSingleton(tableId, tableName, oid);
        }

        public override bool StartRelation(ODBIteratorRelationInfo relationInfo)
        {
            _output?.Dispose();
            _output = OpenOutputStream($"{ToValidFilename(relationInfo.Name)}.txt");

            return base.StartRelation(relationInfo);
        }

        public override void EndRelation()
        {
            _output?.Dispose();

            base.EndRelation();
        }

        public void Dispose()
        {
            _hashAlgorithm?.Dispose();
            _output?.Dispose();
        }
    }
}