namespace BTDB.Encrypted;

public struct EncryptedString
{
    public static implicit operator EncryptedString(string? secret) => new EncryptedString { Secret = secret };
    public static implicit operator string?(EncryptedString secret) => secret.Secret;
    public string? Secret;
    public override string ToString() => Secret;
}
