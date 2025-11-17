using System;
using BTDB.IOC;
using BTDB.ODBLayer;

namespace BTDB.Serialization;

public class ClassMetadata
{
    public Type Type;
    public Type[] Implements;
    public string Name;
    public string Namespace;
    public string? PersistedName;
    public FieldMetadata[] Fields;
    public unsafe delegate*<object> Creator;
    public unsafe delegate*<object, void> OnSerialize;
    public Func<IContainer?, RelationBeforeRemove>? OnBeforeRemoveFactory;

    public string TruePersistedName =>
        PersistedName ?? (string.IsNullOrEmpty(Namespace) ? Name : Namespace + "." + Name);
}
