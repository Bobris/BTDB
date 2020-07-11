using System.Collections.Generic;
using System.Runtime.Serialization;

namespace SimpleTester.TestModel.Events
{
    [DataContract]
    public class ActionFinishedBase<TResult> : Event where TResult: class
    {
        [DataMember(Order = 1)]
        public ulong ActionId { get; set; }
        [DataMember(Order = 2)]
        public string? WorkerId { get; set; }
        [DataMember(Order = 3)]
        public int Version { get; set; }

        [DataMember(Order = 4)]
        public bool IsCanceled { get; set; }
        [DataMember(Order = 5)]
        public bool IsFinal { get; set; }
        [DataMember(Order = 6)]
        public TResult? Result { get; set; }
        [DataMember(Order = 7)]
        public IList<TResult>? Results { get; set; }
    }
}
