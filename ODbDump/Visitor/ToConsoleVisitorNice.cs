using System.Collections.Generic;
using BTDB.ODBLayer;

namespace ODbDump.Visitor
{
    class ToConsoleVisitorNice : ToConsoleFastVisitor, IODBVisitor
    {
        protected string? CurrentFieldName;
        readonly Stack<int> _listItemIndexStack = new Stack<int>();
        int _itemIndex;
        protected int _iid;

        public virtual bool VisitSingleton(uint tableId, string? tableName, ulong oid)
        {
            Print($"Singleton {tableId}-{tableName ?? "?Unknown?"} oid:{oid}");
            return true;
        }

        public virtual bool StartObject(ulong oid, uint tableId, string? tableName, uint version)
        {
            _indent++;
            Print($"Object oid:{oid} {tableId}-{tableName ?? "?Unknown?"} version:{version}");
            return true;
        }

        public bool StartField(string name)
        {
            CurrentFieldName = name;
            return true;
        }

        public virtual bool NeedScalarAsObject()
        {
            return false;
        }

        public virtual void ScalarAsObject(object? content)
        {
        }

        public virtual bool NeedScalarAsText()
        {
            return true;
        }

        public void ScalarAsText(string content)
        {
            Print($"{CurrentFieldName}: {content}");
        }

        public virtual void OidReference(ulong oid)
        {
            Print($"{CurrentFieldName}: Oid#{oid}");
        }

        public virtual bool StartInlineObject(uint tableId, string? tableName, uint version)
        {
            Print($"{CurrentFieldName}: InlineObject {tableId}-{tableName}-{version} ref#{_iid}");
            _indent++;
            return true;
        }

        public void EndInlineObject()
        {
            _indent--;
        }

        public bool StartList()
        {
            Print($"{CurrentFieldName}: Array");
            _listItemIndexStack.Push(_itemIndex);
            _itemIndex = 0;
            _indent++;
            return true;
        }

        public bool StartItem()
        {
            CurrentFieldName = $"[{_itemIndex}]";
            return true;
        }

        public void EndItem()
        {
            _itemIndex++;
        }

        public void EndList()
        {
            _itemIndex = _listItemIndexStack.Pop();
            _indent--;
        }

        public bool StartDictionary(ulong? dicid = null)
        {
            Print($"{CurrentFieldName}: Dictionary" + (dicid.HasValue ? " " + dicid.Value : ""));
            _listItemIndexStack.Push(_itemIndex);
            _itemIndex = 0;
            _indent++;
            return true;
        }

        public bool StartDictKey()
        {
            CurrentFieldName = "Key";
            return true;
        }

        public void EndDictKey()
        {
        }

        public bool StartDictValue()
        {
            CurrentFieldName = "Value";
            return true;
        }

        public void EndDictValue()
        {
            _itemIndex++;
        }

        public void EndDictionary()
        {
            _itemIndex = _listItemIndexStack.Pop();
            _indent--;
        }

        public bool StartSet()
        {
            Print($"{CurrentFieldName}: Set");
            _listItemIndexStack.Push(_itemIndex);
            _itemIndex = 0;
            _indent++;
            return true;
        }

        public bool StartSetKey()
        {
            CurrentFieldName = "Key";
            return true;
        }

        public void EndSetKey()
        {
            _itemIndex++;
        }

        public void EndSet()
        {
            _itemIndex = _listItemIndexStack.Pop();
            _indent--;
        }

        public void EndField()
        {
        }

        public void EndObject()
        {
            _indent--;
        }

        public virtual bool StartRelation(ODBIteratorRelationInfo relationInfo)
        {
            Print($"Relation {relationInfo.Name} {relationInfo.Id}");
            _listItemIndexStack.Push(_itemIndex);
            _itemIndex = 0;
            _indent++;
            return true;
        }

        public bool StartRelationKey()
        {
            Print("Key");
            _indent++;
            return true;
        }

        public void EndRelationKey()
        {
            _indent--;
        }

        public bool StartRelationValue()
        {
            Print("Value");
            _indent++;
            return true;
        }

        public void EndRelationValue()
        {
            _itemIndex++;
            _indent--;
        }

        public virtual void EndRelation()
        {
            _itemIndex = _listItemIndexStack.Pop();
            _indent--;
        }

        public void InlineBackRef(int iid)
        {
            Print($"{CurrentFieldName}: Inline back ref#{iid}");
        }

        public void InlineRef(int iid)
        {
            _iid = iid;
        }

        public virtual bool StartSecondaryIndex(string name)
        {
            return false;
        }

        public virtual void NextSecondaryKey()
        {
        }

        public virtual void EndSecondaryIndex()
        {
        }
    }
}
