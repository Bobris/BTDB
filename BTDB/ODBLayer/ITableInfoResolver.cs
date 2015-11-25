﻿using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    interface ITableInfoResolver
    {
        uint GetLastPersistedVersion(uint id);
        TableVersionInfo LoadTableVersionInfo(uint id, uint version, string tableName);
        long GetSingletonOid(uint id);
        ulong AllocateNewOid();
        IFieldHandlerFactory FieldHandlerFactory { get; }
        ITypeConvertorGenerator TypeConvertorGenerator { get; }
    }
}