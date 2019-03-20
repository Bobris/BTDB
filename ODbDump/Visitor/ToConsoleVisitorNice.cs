using System.Collections.Generic;
using BTDB.ODBLayer;

namespace ODbDump.Visitor
{
    class ToConsoleVisitorNice : ToConsoleFastVisitor, IODBVisitor
    {
        protected string _currentFieldName;
        readonly Stack<int> _listItemIndexStack = new Stack<int>();
        int _itemIndex;
        protected int _iid;

        public virtual bool VisitSingleton(uint tableId, string tableName, ulong oid)
        {
            Print($"Singleton {tableId}-{tableName ?? "?Unknown?"} oid:{oid}");
            return true;
        }

        public virtual bool StartObject(ulong oid, uint tableId, string tableName, uint version)
        {
            _indent++;
            Print($"Object oid:{oid} {tableId}-{tableName ?? "?Unknown?"} version:{version}");
            return true;
        }

        public bool StartField(string name)
        {
            _currentFieldName = name;
            return true;
        }

        public virtual bool NeedScalarAsObject()
        {
            return false;
        }

        public virtual void ScalarAsObject(object content)
        {
        }

        public virtual bool NeedScalarAsText()
        {
            return true;
        }

        public void ScalarAsText(string content)
        {
            Print($"{_currentFieldName}: {content}");
        }

        public virtual void OidReference(ulong oid)
        {
            Print($"{_currentFieldName}: Oid#{oid}");
        }

        public virtual bool StartInlineObject(uint tableId, string tableName, uint version)
        {
            Print($"{_currentFieldName}: InlineObject {tableId}-{tableName}-{version} ref#{_iid}");
            _indent++;
            return true;
        }

        public void EndInlineObject()
        {
            _indent--;
        }

        public bool StartList()
        {
            Print($"{_currentFieldName}: Array");
            _listItemIndexStack.Push(_itemIndex);
            _itemIndex = 0;
            _indent++;
            return true;
        }

        public bool StartItem()
        {
            _currentFieldName = $"[{_itemIndex}]";
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

        public bool StartDictionary()
        {
            Print($"{_currentFieldName}: Dictionary");
            _listItemIndexStack.Push(_itemIndex);
            _itemIndex = 0;
            _indent++;
            return true;
        }

        public bool StartDictKey()
        {
            _currentFieldName = "Key";
            return true;
        }

        public void EndDictKey()
        {
        }

        public bool StartDictValue()
        {
            _currentFieldName = "Value";
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

        public void EndField()
        {
        }

        public void EndObject()
        {
            _indent--;
        }

        public virtual bool StartRelation(string relationName)
        {
            Print($"Relation {relationName}");
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
            Print($"{_currentFieldName}: Inline back ref#{iid}");
        }

        public void InlineRef(int iid)
        {
            _iid = iid;
        }
    }
}