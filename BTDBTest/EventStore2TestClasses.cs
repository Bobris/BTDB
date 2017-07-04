using System;
using System.Collections.Generic;
using Gmc.Cloud.Diagnostic.Db;
using Gmc.Cloud.Infrastructure.Core;

namespace Gmc.Cloud.Infrastructure.Core
{
    public interface IEvent
    {
        ulong Id { get; set; }
        ulong ParentEventId { get; set; }
        ulong UserId { get; set; }
        DateTime Time { get; set; }
        Guid UniqueGuid { get; set; }
        ulong ImpersonatorUserId { get; set; }
        ulong ImpersonatorCompanyId { get; set; }
    }


    public class Event : IEvent
    {
        public ulong Id { get; set; }
        public ulong ParentEventId { get; set; }
        public ulong UserId { get; set; }
        public DateTime Time { get; set; }
        public Guid UniqueGuid { get; set; }
        public ulong ImpersonatorUserId { get; set; }
        public ulong ImpersonatorCompanyId { get; set; }
    }
}

namespace Gmc.Cloud.Diagnostic.Events
{
    public class DiscInfoAdded : Event
    {
        public IList<DbDriveInfo> Infos { get; set; }
        public string InstanceId { get; set; }
    }
}

namespace Gmc.Cloud.Diagnostic.Db
{
    public class DbDriveInfo
    {
        public string Name { get; set; }
        public long TotalSize { get; set; }
        public long AvailableFreeSpace { get; set; }
    }
}