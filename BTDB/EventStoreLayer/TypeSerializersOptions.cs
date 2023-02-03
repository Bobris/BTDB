using BTDB.Encrypted;
using BTDB.FieldHandler;

namespace BTDB.EventStoreLayer;

public class TypeSerializersOptions
{
    public static TypeSerializersOptions Default { get; } = new TypeSerializersOptions
    {
        IgnoreIIndirect = true
    };

    /// <summary>
    /// The value determines whether the <see cref="FieldHandler.IIndirect"/> is serialized or not.
    /// </summary>
    public bool IgnoreIIndirect { get; set; }

    public ISymmetricCipher? SymmetricCipher { get; set; }

    public TypeDescriptorOptions? TypeDescriptorOptions { get; set; }

    public ITypeConvertorGenerator? ConvertorGenerator { get; set; }
}
