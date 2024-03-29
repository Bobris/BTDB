﻿using System;
using System.Reflection;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public class UnresolvedTableFieldInfo : TableFieldInfo
{
    readonly string _handlerName;
    readonly byte[]? _configuration;
    readonly string _tableName;
    readonly FieldHandlerOptions _handlerOptions;

    UnresolvedTableFieldInfo(string name, string handlerName, byte[]? configuration,
        string tableName, FieldHandlerOptions handlerOptions, bool inKeyValue)
        : base(name, null, inKeyValue)
    {
        _handlerName = handlerName;
        _configuration = configuration;
        _tableName = tableName;
        _handlerOptions = handlerOptions;
    }

    internal static UnresolvedTableFieldInfo Load(ref MemReader reader, string tableName,
        FieldHandlerOptions handlerOptions)
    {
        var name = reader.ReadString();
        var inKeyValue = false;
        if (name!.StartsWith('@'))
        {
            inKeyValue = true;
            name = name[1..];
        }

        var handlerName = reader.ReadString();
        var configuration = reader.ReadByteArray();
        return new(name!, handlerName!, configuration, tableName, handlerOptions, inKeyValue);
    }

    internal TableFieldInfo Resolve(IFieldHandlerFactory fieldHandlerFactory)
    {
        var fieldHandler = fieldHandlerFactory.CreateFromName(_handlerName, _configuration, _handlerOptions);
        if (fieldHandler == null)
            throw new BTDBException(
                $"FieldHandlerFactory did not created handler {_handlerName} in {_tableName}.{Name}");
        return Create(Name, fieldHandler, InKeyValue);
    }
}

public class TableFieldInfo : IEquatable<TableFieldInfo>
{
    public readonly string Name;
    public readonly IFieldHandler? Handler;
    public readonly bool InKeyValue;

    protected TableFieldInfo(string name, IFieldHandler? handler, bool inKeyValue)
    {
        Name = name;
        Handler = handler;
        InKeyValue = inKeyValue;
    }

    internal static TableFieldInfo Load(ref MemReader reader, IFieldHandlerFactory fieldHandlerFactory,
        string tableName, FieldHandlerOptions handlerOptions)
    {
        var name = reader.ReadString();
        var inKeyValue = false;
        if (name!.StartsWith('@'))
        {
            inKeyValue = true;
            name = name[1..];
        }

        var handlerName = reader.ReadString();
        var configuration = reader.ReadByteArray();
        var fieldHandler = fieldHandlerFactory.CreateFromName(handlerName!, configuration, handlerOptions);
        if (fieldHandler == null)
            throw new BTDBException(
                $"FieldHandlerFactory did not created handler {handlerName} in {tableName}.{name}");
        return new TableFieldInfo(name!, fieldHandler, inKeyValue);
    }

    internal static TableFieldInfo Create(string name, IFieldHandler handler, bool inKeyValue)
    {
        return new TableFieldInfo(name, handler, inKeyValue);
    }

    public static TableFieldInfo Build(string tableName, PropertyInfo pi, IFieldHandlerFactory fieldHandlerFactory,
        FieldHandlerOptions handlerOptions, bool inKeyValue)
    {
        var fieldHandler = fieldHandlerFactory.CreateFromType(pi.PropertyType, handlerOptions);
        if (fieldHandler == null)
            throw new BTDBException(string.Format(
                "FieldHandlerFactory did not build property {0} of type {2} in {1}", pi.Name, tableName,
                pi.PropertyType.FullName));
        var a = pi.GetCustomAttribute<PersistedNameAttribute>();
        return new TableFieldInfo(a != null ? a.Name : pi.Name, fieldHandler, inKeyValue);
    }

    internal void Save(ref MemWriter writer)
    {
        if (InKeyValue)
        {
            writer.WriteString("@" + Name);
        }
        else
        {
            writer.WriteString(Name);
        }

        writer.WriteString(Handler!.Name);
        writer.WriteByteArray(Handler.Configuration);
    }

    internal static bool Equal(TableFieldInfo a, TableFieldInfo b)
    {
        if (a.Name != b.Name) return false;
        if (a.InKeyValue != b.InKeyValue) return false;
        var ha = a.Handler;
        var hb = b.Handler;
        if (ha == hb) return true;
        if (ha!.Name != hb!.Name) return false;
        var ca = ha.Configuration;
        var cb = hb.Configuration;
        if (ca == cb) return true;
        if (ca == null || cb == null) return false;
        return ca.AsSpan().SequenceEqual(cb);
    }

    public bool Equals(TableFieldInfo other)
    {
        return Equal(this, other);
    }
}
