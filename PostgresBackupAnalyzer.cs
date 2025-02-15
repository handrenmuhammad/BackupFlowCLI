using System.Diagnostics;
using System.Text.RegularExpressions;

namespace DbBackupCLI;

public class PostgresBackupAnalyzer : IBackupAnalyzer
{
    public async Task<(string? timestamp, HashSet<string> databases)> AnalyzeBackup(string archivePath)
    {
        var databases = new HashSet<string>();
        string? timestamp = null;

        // Create a temporary directory for extraction
        var extractDir = Path.Combine(Path.GetTempPath(), $"postgres_analyze_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
        Directory.CreateDirectory(extractDir);

        try
        {
            // extract the tar.gz archive
            await ExtractTarGzArchive(archivePath, extractDir);

            // get the actual directory
            var dumpDir = Directory.GetDirectories(extractDir).First();

            // try to extract timestamp from directory name
            var timestampMatch = Regex.Match(dumpDir, @"(\d{8}_\d{6})");
            if (timestampMatch.Success)
            {
                timestamp = timestampMatch.Groups[1].Value;
            }

            // check for sql dump files (pg_dump format)
            var sqlFiles = Directory.GetFiles(dumpDir, "*.sql");
            if (sqlFiles.Any())
            {
                // this is a pg_dump backup
                foreach (var sqlFile in sqlFiles)
                {
                    var dbName = Path.GetFileNameWithoutExtension(sqlFile);
                    databases.Add(dbName);
                }
            }
            else
            {
                // this might be a pg_basebackup
                var pgVersion = Directory.GetFiles(dumpDir, "PG_VERSION").FirstOrDefault();
                if (pgVersion != null)
                {
                    databases.Add("full_cluster_backup");
                }
            }

            return (timestamp, databases);
        }
        finally
        {
            try
            {
                if (Directory.Exists(extractDir))
                {
                    Directory.Delete(extractDir, true);
                }
            }
            catch
            {
            }
        }
    }

    private async Task ExtractTarGzArchive(string archivePath, string destinationDir)
    {
        var args = $"-xzf \"{archivePath}\" -C \"{destinationDir}\"";

        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "tar",
                Arguments = args,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        var errorBuilder = new System.Text.StringBuilder();

        process.ErrorDataReceived += (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                errorBuilder.AppendLine(e.Data);
            }
        };

        process.Start();
        process.BeginErrorReadLine();

        await process.WaitForExitAsync();

        if (process.ExitCode != 0)
        {
            var error = errorBuilder.ToString();
            throw new Exception($"Failed to extract tar.gz archive: {error}");
        }
    }
}
