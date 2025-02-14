using System.Diagnostics;
using Spectre.Console;

namespace DbBackupCLI;

public class PostgresBackupAnalyzer : IBackupAnalyzer
{
    public async Task<(string? timestamp, HashSet<string> databases)> AnalyzeBackup(string archivePath)
    {
        // Create a temporary directory to extract the backup
        var tempDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempDir);

        try
        {
            // Extract the tar.gz archive
            await ExtractTarGzArchive(archivePath, tempDir);

            // Look for the backup_manifest file
            var manifestPath = Directory.GetFiles(tempDir, "backup_manifest", SearchOption.AllDirectories).FirstOrDefault();
            if (manifestPath == null)
            {
                throw new Exception("backup_manifest file not found in the backup archive");
            }

            // Read and parse the manifest file
            var manifestContent = await File.ReadAllTextAsync(manifestPath);
            var timestamp = ExtractTimestampFromManifest(manifestContent);
            var databases = await GetDatabasesFromPgData(tempDir);

            return (timestamp, databases);
        }
        finally
        {
            // Clean up temporary directory
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
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

    private string? ExtractTimestampFromManifest(string manifestContent)
    {
        // The manifest file contains a line like: "WAL Starting Point: 0/2000028"
        // and "Start Time: 2024-01-01 12:00:00 UTC"
        var startTimeLine = manifestContent.Split('\n')
            .FirstOrDefault(line => line.StartsWith("Start Time:"));

        if (startTimeLine != null)
        {
            var timestampStr = startTimeLine.Replace("Start Time:", "").Trim();
            if (DateTime.TryParse(timestampStr, out var timestamp))
            {
                return timestamp.ToString("yyyy-MM-dd HH:mm:ss UTC");
            }
        }

        return null;
    }

    private async Task<HashSet<string>> GetDatabasesFromPgData(string pgDataDir)
    {
        var databases = new HashSet<string>();
        var baseDir = Path.Combine(pgDataDir, "base");

        if (!Directory.Exists(baseDir))
        {
            throw new Exception("base directory not found in PostgreSQL data directory");
        }

        // Each subdirectory in the base directory represents a database
        var dbDirs = Directory.GetDirectories(baseDir);
        foreach (var dbDir in dbDirs)
        {
            var dbOid = Path.GetFileName(dbDir);
            if (int.TryParse(dbOid, out _))
            {
                // Get database name from global/pg_database
                var dbName = await GetDatabaseNameFromOid(dbOid, pgDataDir);
                if (dbName != null)
                {
                    databases.Add(dbName);
                }
            }
        }

        return databases;
    }

    private async Task<string?> GetDatabaseNameFromOid(string dbOid, string pgDataDir)
    {
        // In a real implementation, we would parse the pg_database file to map OIDs to database names
        // For simplicity, we'll just return a placeholder name based on the OID
        // A proper implementation would require parsing PostgreSQL's binary format
        return $"database_{dbOid}";
    }
}
