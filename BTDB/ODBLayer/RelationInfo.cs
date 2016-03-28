using System;

namespace BTDB.ODBLayer
{
    class RelationInfo
    {
        uint _id;
        string _name;
        IRelationInfoResolver _relationInfoResolver;

        public RelationInfo(uint id, string name, IRelationInfoResolver relationInfoResolver)
        {
            _id = id;
            _name = name;
            _relationInfoResolver = relationInfoResolver;
        }

        public Type ClientType { get; set; }
    }
}