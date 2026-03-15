using System.Collections.Generic;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class CommandHistoryNestedPolymorphicListRegressionTest : ObjectDbTestBase
{
    public CommandHistoryNestedPolymorphicListRegressionTest(ITestOutputHelper output) : base(output)
    {
        _db.RegisterType(typeof(Event));
        _db.RegisterType(typeof(SaveLastChangeEvent));
        _db.RegisterType(typeof(SaveLastChangeEventWithUserName));
        _db.RegisterType(typeof(SaveLastProgramChangeEvent));
        _db.RegisterType(typeof(UndoableEvent));
        _db.RegisterType(typeof(ConnectionConnectorsV3));
        _db.RegisterType(typeof(JourneyMapObjectPosition));
        _db.RegisterType(typeof(AliasedProgramEvent));
    }

    [Generate]
    public class Event
    {
        public ulong Id { get; set; }
        public ulong ParentEventId { get; set; }
        public ulong UserId { get; set; }
        public System.DateTime Time { get; set; }
        public System.DateTime PublishTime { get; set; }
        public System.Guid UniqueGuid { get; set; }
        public ulong ImpersonatorUserId { get; set; }
        public ulong ImpersonatorCompanyId { get; set; }
    }

    public abstract class SaveLastChangeEvent : Event
    {
        public ulong CompanyId { get; set; }

        [NotStored] public virtual ulong LastModifier => UserId;

        [NotStored] public virtual int LastModifierType => 0;

        [NotStored] public System.DateTime Changed => Time;
    }

    public abstract class SaveLastChangeEventWithUserName : SaveLastChangeEvent
    {
        public string UserName { get; set; }
    }

    public abstract class SaveLastProgramChangeEvent : SaveLastChangeEventWithUserName
    {
        public abstract ulong GetProgramId();
    }

    [Generate]
    public abstract class UndoableEvent : SaveLastProgramChangeEvent
    {
        public bool IsUndo { get; set; }
        public bool IsRedo { get; set; }
        public abstract ulong GetCommandHistoryId();

        [NotStored] public bool IsHistoryChange => IsUndo || IsRedo;

        public string SessionId { get; set; }
        public string WindowId { get; set; }
        public override ulong GetProgramId() => GetCommandHistoryId();
    }

    public class JourneyMapObjectPosition
    {
        public ulong PhaseId { get; set; }
        public ulong SwimlaneId { get; set; }
        public int X { get; set; }
        public int Y { get; set; }
    }

    public class ConnectionConnectorsV3
    {
        public ulong StartObjectId { get; set; }
        public int StartConnectionIdx { get; set; }
        public ulong EndObjectId { get; set; }
        public int EndConnectionIdx { get; set; }
    }

    [Generate]
    public class AliasedProgramEvent : UndoableEvent
    {
        public ulong JourneyMapId { get; set; }
        public ulong LinkId { get; set; }
        public ulong LinkProgramId { get; set; }
        public JourneyMapObjectPosition Position { get; set; }
        public IDictionary<ulong, ConnectionConnectorsV3> ConnectionConnectorsV3 { get; set; }

        public ulong ProgramId
        {
            get => JourneyMapId;
            set => JourneyMapId = value;
        }

        public override ulong GetCommandHistoryId() => ProgramId;
    }

    public class CommandHistoryItem
    {
        public ulong EventId { get; set; }
        public List<UndoableEvent> UndoEvents { get; set; }
        public List<UndoableEvent> RedoEvents { get; set; }
    }

    public class CommandHistory
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        public List<CommandHistoryItem> UndoItems { get; set; }
        public List<CommandHistoryItem> RedoItems { get; set; }
    }

    public interface ICommandHistoryTable : IRelation<CommandHistory>
    {
        void Insert(CommandHistory commandHistory);
        CommandHistory? FindByIdOrDefault(ulong companyId, ulong id);
    }

    static AliasedProgramEvent CreateAliasedProgramEvent()
    {
        return new AliasedProgramEvent
        {
            Id = 2,
            ProgramId = 11,
            LinkId = 200,
            LinkProgramId = 201,
            UserId = 7,
            CompanyId = 1,
            UserName = "user",
            SessionId = "session",
            WindowId = "window",
            Position = null,
            ConnectionConnectorsV3 = new Dictionary<ulong, ConnectionConnectorsV3>
            {
                [0] = null
            }
        };
    }

    [Fact]
    public void CanLoadCommandHistoryWithNestedPolymorphicLists()
    {
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<ICommandHistoryTable>();
            table.Insert(new CommandHistory
            {
                CompanyId = 1,
                Id = 42,
                UndoItems =
                [
                    new CommandHistoryItem
                    {
                        EventId = 6,
                        UndoEvents = [CreateAliasedProgramEvent()],
                        RedoEvents = []
                    }
                ],
                RedoItems = []
            });
            tr.Commit();
        }

        ReopenDb();

        _db.RegisterType(typeof(Event));
        _db.RegisterType(typeof(SaveLastChangeEvent));
        _db.RegisterType(typeof(SaveLastChangeEventWithUserName));
        _db.RegisterType(typeof(SaveLastProgramChangeEvent));
        _db.RegisterType(typeof(UndoableEvent));
        _db.RegisterType(typeof(ConnectionConnectorsV3));
        _db.RegisterType(typeof(JourneyMapObjectPosition));
        _db.RegisterType(typeof(AliasedProgramEvent));

        using var tr2 = _db.StartTransaction();
        var loaded = tr2.GetRelation<ICommandHistoryTable>().FindByIdOrDefault(1, 42);
        Assert.NotNull(loaded);
        Assert.Single(loaded.UndoItems);
        Assert.Single(loaded.UndoItems[0].UndoEvents);
        Assert.IsType<AliasedProgramEvent>(loaded.UndoItems[0].UndoEvents[0]);
    }
}
