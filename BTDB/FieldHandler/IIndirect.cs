namespace BTDB.FieldHandler
{
    public interface IIndirect<T> where T : class
    {
        T Value { get; set; }
    }
}