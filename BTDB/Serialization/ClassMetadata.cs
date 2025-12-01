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
    public uint[]? PrimaryKeyFields;
    public uint IndexOfInKeyValue; // If it is PrimaryKeyFields.Length then there is no in key values
    public (string Name, uint[] SecondaryKeyFields)[]? SecondaryKeys;

    public string TruePersistedName =>
        PersistedName ?? (string.IsNullOrEmpty(Namespace) ? Name : Namespace + "." + Name);
}
