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
    static void Main()
    {
        MainAsync().Wait();
    }

    static async Task<int> MainAsync()
    {
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
        if (workDirChangesCount > 0)
        {
            Console.WriteLine("DANGER! THERE ARE " + workDirChangesCount + " CHANGES IN WORK DIR!");
        }

        var topVersionLine = Array.IndexOf(logLines, topVersion);
        var lastVersionNumber = new System.Version(lastVersion[3..]);
        var patchVersionNumber =
            new System.Version(lastVersionNumber.Major, lastVersionNumber.Minor, lastVersionNumber.Build + 1);
        var minorVersionNumber = new System.Version(lastVersionNumber.Major, lastVersionNumber.Minor + 1, 0);
        var majorVersionNumber = new System.Version(lastVersionNumber.Major + 1, 0, 0);
        Console.WriteLine("Press 1 for Major " + majorVersionNumber.ToString(3));
        Console.WriteLine("Press 2 for Minor " + minorVersionNumber.ToString(3));
        Console.WriteLine("Press 3 for Patch " + patchVersionNumber.ToString(3));
        var choice = Console.ReadKey().KeyChar;
        Console.WriteLine();
        if (choice < '1' || choice > '3')
        {
            Console.WriteLine("Not pressed 1, 2 or 3. Exiting.");
            return 1;
        }

        if (choice == '1')
            lastVersionNumber = majorVersionNumber;
        if (choice == '2')
            lastVersionNumber = minorVersionNumber;
        if (choice == '3')
            lastVersionNumber = patchVersionNumber;
        var newVersion = lastVersionNumber.ToString(3);
        Console.WriteLine("Building version " + newVersion);
        await UpdateCsProj(projDir, newVersion);
        var outputLogLines = logLines.ToList();
        var releaseLogLines = logLines.Skip(topVersionLine + 1).SkipWhile(string.IsNullOrWhiteSpace)
            .TakeWhile(s => !s.StartsWith("## ")).ToList();
        while (releaseLogLines.Count > 0 && string.IsNullOrWhiteSpace(releaseLogLines[^1]))
            releaseLogLines.RemoveAt(releaseLogLines.Count - 1);
        outputLogLines.Insert(topVersionLine + 1, "## " + newVersion);
        outputLogLines.Insert(topVersionLine + 1, "");
        if (Directory.Exists(projDir + "/BTDB/bin/Release"))
            Directory.Delete(projDir + "/BTDB/bin/Release", true);
        if (Directory.Exists(projDir + "/BTDB.SourceGenerator/bin/Release"))
            Directory.Delete(projDir + "/BTDB.SourceGenerator/bin/Release", true);
        if (Directory.Exists(projDir + "/ODbDump/bin/Release"))
            Directory.Delete(projDir + "/ODbDump/bin/Release", true);
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

        Build(projDir, newVersion, nugetToken);
        BuildSourceGenerator(projDir, newVersion, nugetToken);
        BuildODbDump(projDir);

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
        Commands.Stage(gitrepo, "BTDB/BTDB.csproj");
        Commands.Stage(gitrepo, "BTDB.SourceGenerator/BTDB.SourceGenerator.csproj");
        Commands.Stage(gitrepo, "ODbDump/ODbDump.csproj");
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
        var release = new NewRelease(newVersion);
        release.Name = newVersion;
        release.Body = string.Join("", releaseLogLines.Select(s => s + '\n'));
        var release2 = await client.Repository.Release.Create(btdbRepo.Id, release);
        Console.WriteLine("release url:");
        Console.WriteLine(release2.HtmlUrl);
        var uploadAsset = await UploadWithRetry(projDir + "/BTDB/bin/Release/", client, release2, "BTDB.zip");
        Console.WriteLine("BTDB url:");
        Console.WriteLine(uploadAsset.BrowserDownloadUrl);
        uploadAsset = await UploadWithRetry(projDir + "/ODbDump/bin/Release/", client, release2, "ODbDump.zip");
        Console.WriteLine("ODbDump url:");
        Console.WriteLine(uploadAsset.BrowserDownloadUrl);
        Console.WriteLine("Press Enter for finish");
        Console.ReadLine();
        return 0;
    }

    static async Task UpdateCsProj(string projDir, string newVersion)
    {
        var fn = projDir + "/BTDB/BTDB.csproj";
        var content = await File.ReadAllTextAsync(fn);
        content = new Regex("<Version>.+</Version>").Replace(content, "<Version>" + newVersion + "</Version>");
        await File.WriteAllTextAsync(fn, content, new UTF8Encoding(false));
        fn = projDir + "/BTDB.SourceGenerator/BTDB.SourceGenerator.csproj";
        content = await File.ReadAllTextAsync(fn);
        content = new Regex("<Version>.+</Version>").Replace(content, "<Version>" + newVersion + "</Version>");
        await File.WriteAllTextAsync(fn, content, new UTF8Encoding(false));
        fn = projDir + "/ODbDump/ODbDump.csproj";
        content = await File.ReadAllTextAsync(fn);
        content = new Regex("<Version>.+</Version>").Replace(content, "<Version>" + newVersion + "</Version>");
        await File.WriteAllTextAsync(fn, content, new UTF8Encoding(false));
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

    static void Build(string projDir, string newVersion, string nugetToken)
    {
        var start = new ProcessStartInfo("dotnet", "pack -c Release")
        {
            UseShellExecute = true,
            WorkingDirectory = projDir + "/BTDB"
        };
        var process = Process.Start(start);
        process!.WaitForExit();
        var source = projDir + "/BTDB";
        var releaseSources = projDir + "/BTDB/bin/Release/Sources";
        foreach (var fn in Directory.GetFiles(source, "*.*", SearchOption.AllDirectories).ToList())
        {
            var relfn = fn.Substring(source.Length + 1);
            if (relfn.StartsWith("bin")) continue;
            if (relfn.StartsWith("obj")) continue;
            Directory.CreateDirectory(Path.GetDirectoryName(releaseSources + "/" + relfn)!);
            File.Copy(fn, releaseSources + "/" + relfn);
        }

        System.IO.Compression.ZipFile.CreateFromDirectory(releaseSources, projDir + "/BTDB/bin/Release/BTDB.zip",
            System.IO.Compression.CompressionLevel.Optimal, false);
        start = new("dotnet", "nuget push BTDB." + newVersion + ".nupkg -s https://nuget.org -k " + nugetToken)
        {
            UseShellExecute = true,
            WorkingDirectory = projDir + "/BTDB/bin/Release"
        };
        process = Process.Start(start);
        process!.WaitForExit();
    }

    static void BuildSourceGenerator(string projDir, string newVersion, string nugetToken)
    {
        var start = new ProcessStartInfo("dotnet", "pack -c Release")
        {
            UseShellExecute = true,
            WorkingDirectory = projDir + "/BTDB.SourceGenerator"
        };
        var process = Process.Start(start);
        process!.WaitForExit();
        start = new("dotnet",
            "nuget push BTDB.SourceGenerator." + newVersion + ".nupkg -s https://nuget.org -k " + nugetToken)
        {
            UseShellExecute = true,
            WorkingDirectory = projDir + "/BTDB.SourceGenerator/bin/Release"
        };
        process = Process.Start(start);
        process!.WaitForExit();
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
        var source = projDir + "/ODbDump/bin/Release/net8.0/publish";
        System.IO.Compression.ZipFile.CreateFromDirectory(source,
            projDir + "/ODbDump/bin/Release/ODbDump.zip",
            System.IO.Compression.CompressionLevel.Optimal, false);
    }
}
