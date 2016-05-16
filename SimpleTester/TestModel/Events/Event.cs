using System;
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events
{
    [DataContract]
    public abstract class Event
    {
        [DataMember(Order = 1)]
        public ulong Id { get; set; }

        [DataMember(Order = 2)]
        public ulong ParentEventId { get; set; }

        [DataMember(Order = 3)]
        public ulong UserId { get; set; }

        [DataMember(Order = 4)]
        public DateTime Time { get; set; }

        [DataMember(Order = 5)]
        public Guid UniqueGuid { get; set; }
    }
}
