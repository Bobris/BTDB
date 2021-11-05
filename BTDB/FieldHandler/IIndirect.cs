namespace BTDB.FieldHandler;

public interface IIndirect
{
    ulong Oid { get; }
    object ValueAsObject { get; }
}

public interface IIndirect<T> : IIndirect where T : class
{
    T Value { get; set; }
}
