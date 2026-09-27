using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using BTDB.Replication.Azure;
using BTDB.Replication.Http;
using Xunit;

namespace BTDB.Replication.ProcessTests;

public class PublicHostingApiTest
{
    [Fact]
    public void SubprocessHostHasNoPrivilegedAccessToAnyReplicationAssembly()
    {
        var consumer = typeof(Program).Assembly.GetName().Name;
        foreach (var assembly in new[] { typeof(IReplicationNodeHost).Assembly, typeof(ReplicationHosting).Assembly,
                     typeof(AzureLeaderStorage).Assembly })
            Assert.DoesNotContain(assembly.GetCustomAttributes<InternalsVisibleToAttribute>(),
                attribute => new AssemblyName(attribute.AssemblyName).Name == consumer);
    }

    [Fact]
    public void ApplicationCannotManufactureOrRenewAuthorityThroughThePublicApi()
    {
        Assert.Empty(typeof(LeaseAuthority).GetConstructors());
        var publicMethods = typeof(LeaseAuthority).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
        Assert.DoesNotContain(publicMethods, method => method.Name is "BeginRequest" or "AcceptSuccess");
        var assembly = typeof(IReplicationNodeHost).Assembly;
        foreach (var name in new[] { "ReplicationNodeCoordinator", "LeaseSessionController", "LeaderSelection",
                     "LeadershipActivation", "LeadershipSession", "FollowerComparisonSession" })
            Assert.False(assembly.GetType("BTDB.Replication." + name)!.IsVisible);
    }

    [Fact]
    public void CredentialBearingPublicRecordsDoNotPrintSecrets()
    {
        const string secret = "do-not-log-this-credential";
        object[] records =
        [
            new LeaderCandidate("cluster", "node", "session", 1, ["main"], "https://node.example", secret),
            new LeaderRecord("etag", "{\"apiKey\":\"" + secret + "\"}"),
            new LeaseGrant(secret, TimeSpan.FromSeconds(15))
        ];
        Assert.All(records, record => Assert.DoesNotContain(secret, record.ToString()));
        Assert.DoesNotContain("apiKey", records[1].ToString());
    }
}
