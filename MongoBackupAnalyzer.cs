using System.Diagnostics;

namespace DbBackupCLI;

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
            Console.WriteLine($"Mongorestore error output: {error}");
        }

        Console.WriteLine("Mongorestore output:");
        Console.WriteLine(output);

        return ParseMongoRestoreOutput(error);
    }

    private static (string? timestamp, HashSet<string> databases) ParseMongoRestoreOutput(string output)
    {
        var databaseNames = new HashSet<string>();
        string? backupTimestamp = null;

        foreach (var line in output.Split('\n'))
        {
            Console.WriteLine($"Processing line: {line}");

            if (line.Contains("found collection"))
            {
                var timeMatch = System.Text.RegularExpressions.Regex.Match(line, @"^([\d-]+T[\d:.]+[+-]\d{4})");
                if (timeMatch.Success && backupTimestamp == null)
                {
                    backupTimestamp = timeMatch.Groups[1].Value;
                    Console.WriteLine($"Found timestamp: {backupTimestamp}");
                }

                var dbMatch = System.Text.RegularExpressions.Regex.Match(line, @"found collection ([\w-]+)\.");
                if (dbMatch.Success)
                {
                    var dbName = dbMatch.Groups[1].Value;
                    databaseNames.Add(dbName);
                    Console.WriteLine($"Found database: {dbName}");
                }
            }
        }

        if (!databaseNames.Any())
        {
            // Try alternative pattern matching if no databases found
            foreach (var line in output.Split('\n'))
            {
                var dbMatch = System.Text.RegularExpressions.Regex.Match(line, @"restoring ([\w-]+)\.");
                if (dbMatch.Success)
                {
                    var dbName = dbMatch.Groups[1].Value;
                    databaseNames.Add(dbName);
                    Console.WriteLine($"Found database (alternative pattern): {dbName}");
                }
            }
        }

        return (backupTimestamp, databaseNames);
    }
}
