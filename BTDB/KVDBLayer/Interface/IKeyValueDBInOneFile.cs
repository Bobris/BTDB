namespace BTDB.KVDBLayer
{
    public interface IKeyValueDBInOneFile
    {
        // Default Cache Size is 10MB. Minimum is 0MB (But it is special value usable for Cache eviction hardening, so normaly use atleast 1MB). Maximum is 1024MB.
        int CacheSizeInMB { get; set; }

        // It will be UTF-8 encoded. There is atleast 300 bytes to store your description. If you will try to set too long description you will get exception.
        // DB must be in Open state to be able get or set description. It is not protected by transaction so don't be too much dependent on it.
        string HumanReadableDescriptionInHeader { get; set; }
    }
}