#nullable enable annotations

using LibGit2Sharp;
using Octokit;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Releaser;

static class Program
{
    static Task<int> Main(string[] args)
    {
        return MainAsync(args);
    }

    static async Task<int> MainAsync(string[] args)
    {
        var prepare = args is ["prepare", ..];
        if (prepare) args = args[1..];
        var choice = args switch
        {
            [] => prepare ? '3' : '\0',
            ["major"] => '1',
            ["minor"] => '2',
            ["patch"] => '3',
            _ => '?'
        };
        if (choice == '?')
        {
            Console.WriteLine("Usage: Releaser [major|minor|patch] | prepare [major|minor|patch]");
            return 1;
        }

        var projDir = Environment.CurrentDirectory;
        while (!File.Exists(projDir + "/CHANGELOG.md"))
        {
            projDir = Path.GetDirectoryName(projDir);
            if (string.IsNullOrWhiteSpace(projDir))
            {
                Console.WriteLine("Cannot find CHANGELOG.md in some parent directory");
                return 1;
            }
        }

        Console.WriteLine("Project root directory: " + projDir);
        var logLines = await File.ReadAllLinesAsync(projDir + "/CHANGELOG.md");
        var topVersion = logLines.FirstOrDefault(s => s.StartsWith("## "));
        var lastVersion = logLines.Where(s => s.StartsWith("## ")).Skip(1).FirstOrDefault();
        if (logLines.Length < 5)
        {
            Console.WriteLine("CHANGELOG.md has less than 5 lines");
            return 1;
        }

        if (topVersion != "## [unreleased]")
        {
            Console.WriteLine("Top version should be ## [unreleased]");
            return 1;
        }

        if (lastVersion == null)
        {
            Console.WriteLine("Cannot find previous version");
            return 1;
        }

        using var gitrepo = new LibGit2Sharp.Repository(projDir);
        int workDirChangesCount;
        using (var workDirChanges = gitrepo.Diff.Compare<TreeChanges>())
            workDirChangesCount = workDirChanges.Count;
        if (!prepare && workDirChangesCount > 0)
        {
            Console.WriteLine("DANGER! THERE ARE " + workDirChangesCount + " CHANGES IN WORK DIR!");
        }

        var topVersionLine = Array.IndexOf(logLines, topVersion);
        var lastVersionNumber = new System.Version(lastVersion[3..]);
        var patchVersionNumber =
            new System.Version(lastVersionNumber.Major, lastVersionNumber.Minor, lastVersionNumber.Build + 1);
        var minorVersionNumber = new System.Version(lastVersionNumber.Major, lastVersionNumber.Minor + 1, 0);
        var majorVersionNumber = new System.Version(lastVersionNumber.Major + 1, 0, 0);
        if (choice == '\0')
        {
            Console.WriteLine("Press 1 for Major " + majorVersionNumber.ToString(3));
            Console.WriteLine("Press 2 for Minor " + minorVersionNumber.ToString(3));
            Console.WriteLine("Press 3 for Patch " + patchVersionNumber.ToString(3));
            Console.WriteLine("Press 4 for Nuget repush");
            choice = Console.ReadKey().KeyChar;
            Console.WriteLine();
        }
        if (choice < '1' || choice > '4')
        {
            Console.WriteLine("Not pressed 1, 2, 3 or 4. Exiting.");
            return 1;
        }

        if (choice == '1')
            lastVersionNumber = majorVersionNumber;
        if (choice == '2')
            lastVersionNumber = minorVersionNumber;
        if (choice == '3')
            lastVersionNumber = patchVersionNumber;
        var newVersion = lastVersionNumber.ToString(3);
        if (prepare)
        {
            var outputDirectory = Path.Combine(projDir, "artifacts", "package", "prepare", newVersion);
            foreach (var project in PackageProjects)
                PackProject(projDir, project, newVersion, outputDirectory);
            Console.WriteLine("Prepared packages: " + outputDirectory);
            Console.WriteLine("No packages published; project versions, changelog, commits and tags are unchanged.");
            return 0;
        }

        Console.WriteLine("Building version " + newVersion);
        await UpdateCsProj(projDir, newVersion);
        var outputLogLines = logLines.ToList();
        var releaseLogLines = logLines.Skip(topVersionLine + 1).SkipWhile(string.IsNullOrWhiteSpace)
            .TakeWhile(s => !s.StartsWith("## ")).ToList();
        while (releaseLogLines.Count > 0 && string.IsNullOrWhiteSpace(releaseLogLines[^1]))
            releaseLogLines.RemoveAt(releaseLogLines.Count - 1);
        outputLogLines.Insert(topVersionLine + 1, "## " + newVersion);
        outputLogLines.Insert(topVersionLine + 1, "");
        if (Directory.Exists(projDir + "/artifacts"))
            Directory.Delete(projDir + "/artifacts", true);
        var fileNameOfNugetToken =
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile) + "/.nuget/token.txt";
        string nugetToken;
        try
        {
            nugetToken = File.ReadAllLines(fileNameOfNugetToken).First();
        }
        catch
        {
            Console.WriteLine("Cannot read nuget token from " + fileNameOfNugetToken);
            return 1;
        }

        foreach (var project in PackageProjects)
            PackProject(projDir, project);
        ZipBTDBSources(projDir);
        BuildODbDump(projDir);
        PublishPackages(projDir, newVersion, nugetToken);

        if (choice == '4') return 0;
        var client = new GitHubClient(new ProductHeaderValue("BTDB-releaser"));
        client.SetRequestTimeout(TimeSpan.FromMinutes(15));
        var fileNameOfGithubToken =
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile) + "/.github/token.txt";
        string githubToken;
        try
        {
            githubToken = File.ReadAllLines(fileNameOfGithubToken).First();
        }
        catch
        {
            Console.WriteLine("Cannot read github token from " + fileNameOfGithubToken);
            return 1;
        }

        client.Credentials = new(githubToken);
        var btdbRepo = (await client.Repository.GetAllForUser("bobris")).First(r => r.Name == "BTDB");
        Console.WriteLine("BTDB repo id: " + btdbRepo.Id);
        await File.WriteAllTextAsync(projDir + "/CHANGELOG.md", string.Join("", outputLogLines.Select(s => s + '\n')));
        Commands.Stage(gitrepo, "CHANGELOG.md");
        foreach (var project in VersionedProjects)
            Commands.Stage(gitrepo, project + "/" + project + ".csproj");
        var author = new LibGit2Sharp.Signature("Releaser", "boris.letocha@gmail.com", DateTime.Now);
        gitrepo.Commit("Released " + newVersion, author, author);
        gitrepo.ApplyTag(newVersion);
        var options = new PushOptions();
        options.CredentialsProvider = (_, _, _) =>
            new UsernamePasswordCredentials
            {
                Username = githubToken,
                Password = ""
            };
        gitrepo.Network.Push(gitrepo.Head, options);
        gitrepo.Network.Push(gitrepo.Network.Remotes["origin"], "refs/tags/" + newVersion, options);
        var release = new NewRelease(newVersion);
        release.Name = newVersion;
        release.TargetCommitish = gitrepo.Head.Tip.Sha;
        release.Body = string.Join("", releaseLogLines.Select(s => s + '\n'));
        var release2 = await client.Repository.Release.Create(btdbRepo.Id, release);
        Console.WriteLine("release url:");
        Console.WriteLine(release2.HtmlUrl);
        var uploadAsset = await UploadWithRetry(projDir + "/artifacts/bin/BTDB/Release/", client, release2, "BTDB.zip");
        Console.WriteLine("BTDB url:");
        Console.WriteLine(uploadAsset.BrowserDownloadUrl);
        uploadAsset =
            await UploadWithRetry(projDir + "/artifacts/bin/ODbDump/Release/", client, release2, "ODbDump.zip");
        Console.WriteLine("ODbDump url:");
        Console.WriteLine(uploadAsset.BrowserDownloadUrl);
        return 0;
    }

    static async Task UpdateCsProj(string projDir, string newVersion)
    {
        var version = new Regex("<Version>.+</Version>");
        foreach (var project in VersionedProjects)
        {
            var fn = projDir + "/" + project + "/" + project + ".csproj";
            var content = await File.ReadAllTextAsync(fn);
            content = version.Replace(content, "<Version>" + newVersion + "</Version>");
            await File.WriteAllTextAsync(fn, content, new UTF8Encoding(false));
        }
    }

    static async Task<ReleaseAsset> UploadWithRetry(string projDir, GitHubClient client, Release release2,
        string fileName)
    {
        for (var i = 0; i < 5; i++)
        {
            try
            {
                return await client.Repository.Release.UploadAsset(release2,
                    new ReleaseAssetUpload(fileName, "application/zip", File.OpenRead(projDir + fileName),
                        TimeSpan.FromMinutes(14)));
            }
            catch (Exception)
            {
                Console.WriteLine("Upload Asset " + fileName + " failed " + i);
            }
        }

        throw new OperationCanceledException("Upload Asset " + fileName + " failed");
    }

    static void ZipBTDBSources(string projDir)
    {
        var source = projDir + "/BTDB";
        var releaseSources = projDir + "/artifacts/bin/BTDB/release/Sources";
        foreach (var fn in Directory.GetFiles(source, "*.*", SearchOption.AllDirectories).ToList())
        {
            var relfn = fn.Substring(source.Length + 1);
            if (relfn.StartsWith("bin")) continue;
            if (relfn.StartsWith("obj")) continue;
            Directory.CreateDirectory(Path.GetDirectoryName(releaseSources + "/" + relfn)!);
            File.Copy(fn, releaseSources + "/" + relfn);
        }

        System.IO.Compression.ZipFile.CreateFromDirectory(releaseSources,
            projDir + "/artifacts/bin/BTDB/release/BTDB.zip",
            System.IO.Compression.CompressionLevel.Optimal, false);
    }

    // All packages are released with the same version, without a prerelease suffix.
    static readonly string[] PackageProjects =
    [
        "BTDB", "BTDB.SourceGenerator", "BTDB.AzureStorage", "BTDB.Replication", "BTDB.Replication.Azure",
        "BTDB.Replication.Http", "BTDB.Replication.EventSourcing", "BTDB.BackendActions", "BTDB.LSM", "BTDB.LSM.Azure"
    ];

    // Projects whose <Version> the release updates and commits.
    static readonly string[] VersionedProjects = [.. PackageProjects, "ODbDump"];

    static void PackProject(string projDir, string project, string? version = null, string? outputDirectory = null)
    {
        var start = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = Path.Combine(projDir, project)
        };
        start.ArgumentList.Add("pack");
        start.ArgumentList.Add("-c");
        start.ArgumentList.Add("Release");
        if (version != null)
            start.ArgumentList.Add("-p:Version=" + version);
        if (outputDirectory != null)
        {
            start.ArgumentList.Add("--output");
            start.ArgumentList.Add(outputDirectory);
        }
        using var pack = Process.Start(start);
        pack!.WaitForExit();
        if (pack.ExitCode != 0)
            throw new InvalidOperationException("Packing " + project + " failed with exit code " + pack.ExitCode);
    }

    static void PublishPackages(string projDir, string newVersion, string nugetToken)
    {
        foreach (var project in PackageProjects)
        {
            var start = new ProcessStartInfo("dotnet")
            {
                WorkingDirectory = projDir + "/artifacts/package/release"
            };
            start.ArgumentList.Add("nuget");
            start.ArgumentList.Add("push");
            start.ArgumentList.Add(project + "." + newVersion + ".nupkg");
            start.ArgumentList.Add("-s");
            start.ArgumentList.Add("https://nuget.org");
            start.ArgumentList.Add("-k");
            start.ArgumentList.Add(nugetToken);
            using var push = Process.Start(start);
            push!.WaitForExit();
            if (push.ExitCode != 0)
                throw new InvalidOperationException("Publishing " + project + " failed with exit code " + push.ExitCode);
        }
    }

    static void BuildODbDump(string projDir)
    {
        var start = new ProcessStartInfo("dotnet", "publish -c Release")
        {
            UseShellExecute = true,
            WorkingDirectory = projDir + "/ODbDump"
        };
        var process = Process.Start(start);
        process!.WaitForExit();
        if (process.ExitCode != 0)
            throw new InvalidOperationException("dotnet command failed with exit code " + process.ExitCode);
        var source = projDir + "/artifacts/publish/ODbDump/Release";
        System.IO.Compression.ZipFile.CreateFromDirectory(source,
            projDir + "/artifacts/bin/ODbDump/Release/ODbDump.zip",
            System.IO.Compression.CompressionLevel.Optimal, false);
    }
}
