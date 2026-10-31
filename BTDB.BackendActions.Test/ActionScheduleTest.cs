using Xunit;

namespace BTDB.BackendActions.Test;

public class ActionScheduleTest
{
    static DateTime At(int month, int day, int hour, int minute) => new(2026, month, day, hour, minute, 0, DateTimeKind.Utc);

    [Fact]
    public void RepeatedOccurrencesCountFromMidnight()
    {
        var every90 = new RepeatedlyScheduledAttribute(90);
        Assert.Equal(At(10, 4, 0, 0), every90.LatestOccurrence(At(10, 4, 1, 29)));
        Assert.Equal(At(10, 4, 1, 30), every90.LatestOccurrence(At(10, 4, 1, 30)));
        Assert.Equal(At(10, 4, 22, 30), every90.LatestOccurrence(At(10, 4, 23, 59)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RepeatedlyScheduledAttribute(0));
    }

    [Fact]
    public void DailyAndMonthlyOccurrencesStepBackUntilTheyPass()
    {
        var daily = new DailyScheduledAttribute(6 * 60);
        Assert.Equal(At(10, 3, 6, 0), daily.LatestOccurrence(At(10, 4, 5, 59)));
        Assert.Equal(At(10, 4, 6, 0), daily.LatestOccurrence(At(10, 4, 6, 0)));
        var monthly = new MonthlyScheduledAttribute(31, 0);
        Assert.Equal(At(9, 30, 0, 0), monthly.LatestOccurrence(At(10, 30, 23, 0))); // September has 30 days
        Assert.Equal(At(10, 31, 0, 0), monthly.LatestOccurrence(At(10, 31, 0, 0)));
        Assert.Equal(At(1, 31, 0, 0), new MonthlyScheduledAttribute(31, 0).LatestOccurrence(At(2, 15, 0, 0)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new DailyScheduledAttribute(1440));
    }
}
