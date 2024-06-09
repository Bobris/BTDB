using System;
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
        string tableName, FieldHandlerOptions handlerOptions, bool inKeyValue, bool computed)
        : base(name, null, inKeyValue, computed)
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

        var computed = false;
        if (name!.StartsWith('#'))
        {
            computed = true;
            name = name[1..];
        }

        var handlerName = reader.ReadString();
        var configuration = reader.ReadByteArray();
        return new(name!, handlerName!, configuration, tableName, handlerOptions, inKeyValue, computed);
    }

    internal TableFieldInfo Resolve(IFieldHandlerFactory fieldHandlerFactory)
    {
        var fieldHandler = fieldHandlerFactory.CreateFromName(_handlerName, _configuration, _handlerOptions);
        if (fieldHandler == null)
            throw new BTDBException(
                $"FieldHandlerFactory did not created handler {_handlerName} in {_tableName}.{Name}");
        return Create(Name, fieldHandler, InKeyValue, Computed);
    }
}

public class TableFieldInfo : IEquatable<TableFieldInfo>
{
    public readonly string Name;
    public readonly IFieldHandler? Handler;
    public readonly bool InKeyValue;
    public readonly bool Computed;

    protected TableFieldInfo(string name, IFieldHandler? handler, bool inKeyValue, bool computed)
    {
        Name = name;
        Handler = handler;
        InKeyValue = inKeyValue;
        Computed = computed;
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

        var computed = false;
        if (name!.StartsWith('#'))
        {
            computed = true;
            name = name[1..];
        }

        var handlerName = reader.ReadString();
        var configuration = reader.ReadByteArray();
        var fieldHandler = fieldHandlerFactory.CreateFromName(handlerName!, configuration, handlerOptions);
        if (fieldHandler == null)
            throw new BTDBException(
                $"FieldHandlerFactory did not created handler {handlerName} in {tableName}.{name}");
        return new(name!, fieldHandler, inKeyValue, computed);
    }

    internal static TableFieldInfo Create(string name, IFieldHandler handler, bool inKeyValue, bool computed)
    {
        return new(name, handler, inKeyValue, computed);
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
        return new(a != null ? a.Name : pi.Name, fieldHandler, inKeyValue, !pi.CanWrite);
    }

    internal void Save(ref MemWriter writer)
    {
        if (Computed)
        {
            writer.WriteString("#" + Name);
        }
        else if (InKeyValue)
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
        if (a.Computed != b.Computed) return false;
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
