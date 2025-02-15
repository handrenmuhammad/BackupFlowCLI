using System.Diagnostics;
using Spectre.Console;

namespace BackupFlowCLI;

public class MongoBackupAnalyzer : IBackupAnalyzer
{
    public async Task<(string? timestamp, HashSet<string> databases)> AnalyzeBackup(string archivePath)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "mongorestore",
                Arguments = $"--archive=\"{archivePath}\" --gzip --dryRun --verbose",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        process.Start();

        var outputTask = process.StandardOutput.ReadToEndAsync();
        var errorTask = process.StandardError.ReadToEndAsync();

        await Task.WhenAll(
            Task.Run(() => process.WaitForExit()),
            outputTask,
            errorTask
        );

        var output = await outputTask;
        var error = await errorTask;

        if (!string.IsNullOrEmpty(error))
        {
            AnsiConsole.MarkupLine($"[red]Mongorestore error output: {error}[/]");
        }

        AnsiConsole.MarkupLine($"[yellow]Mongorestore output:[/]");
        AnsiConsole.MarkupLine(output);

        return DisplayDatabases(output);
    }

    private static (string? timestamp, HashSet<string> databases) DisplayDatabases(string output)
    {
        var databaseNames = new HashSet<string>();
        string? backupTimestamp = null;

        foreach (var line in output.Split('\n'))
        {
            AnsiConsole.MarkupLine($"[yellow]Processing line: {line}[/]");

            if (line.Contains("found collection"))
            {
                var timeMatch = System.Text.RegularExpressions.Regex.Match(line, @"^([\d-]+T[\d:.]+[+-]\d{4})");
                if (timeMatch.Success && backupTimestamp == null)
                {
                    backupTimestamp = timeMatch.Groups[1].Value;
                }

                var dbMatch = System.Text.RegularExpressions.Regex.Match(line, @"found collection ([\w-]+)\.");
                if (dbMatch.Success)
                {
                    var dbName = dbMatch.Groups[1].Value;
                    databaseNames.Add(dbName);
                }
            }
        }

        if (databaseNames.Count == 0)
        {
            foreach (var line in output.Split('\n'))
            {
                var dbMatch = System.Text.RegularExpressions.Regex.Match(line, @"restoring ([\w-]+)\.");
                if (dbMatch.Success)
                {
                    var dbName = dbMatch.Groups[1].Value;
                    databaseNames.Add(dbName);
                    AnsiConsole.MarkupLine($"[yellow]Found database (alternative pattern): {dbName}[/]");
                }
            }
        }

        return (backupTimestamp, databaseNames);
    }
}
