using System;
using System.Diagnostics.CodeAnalysis;
using BTDB.IOC;
using BTDB.ODBLayer;

namespace BTDB.Serialization;

public class ClassMetadata
{
    // Generated code assigns typeof(T) here, so trimming keeps the members ObjectDB and the event serializers still
    // reflect over, also for types in library assemblies.
    [DynamicallyAccessedMembers(StoredMembers)] public Type Type;

    internal const DynamicallyAccessedMemberTypes StoredMembers =
        DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.NonPublicProperties |
        DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.NonPublicFields |
        DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.NonPublicConstructors |
        DynamicallyAccessedMemberTypes.Interfaces;
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
