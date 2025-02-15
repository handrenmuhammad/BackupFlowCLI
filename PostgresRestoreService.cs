using System.Diagnostics;
using Spectre.Console;

namespace BackupFlowCLI;

public class PostgresRestoreService
{
    private readonly string _host;
    private readonly int _port;
    private readonly string _username;
    private readonly string _password;
    private readonly string? _connectionString;

    public PostgresRestoreService(string host, int port, string username, string password)
    {
        _host = host;
        _port = port;
        _username = username;
        _password = password;
        _connectionString = null;
    }

    public PostgresRestoreService(string connectionString)
    {
        _connectionString = connectionString;

        // Parse connection string to get individual components
        var csb = new Npgsql.NpgsqlConnectionStringBuilder(connectionString);
        _host = csb.Host ?? "localhost";
        _port = csb.Port;
        _username = csb.Username ?? "";
        _password = csb.Password ?? "";
    }

    public async Task RestoreBackup(string backupPath, string[]? databases = null)
    {
        // Extract the backup archive
        var extractDir = Path.Combine(Path.GetTempPath(), $"postgres_restore_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
        Directory.CreateDirectory(extractDir);

        try
        {
            // Extract the tar.gz archive
            await ExtractTarGzArchive(backupPath, extractDir);

            // Get the actual dump directory (it should be the only subdirectory)
            var dumpDir = Directory.GetDirectories(extractDir).First();
            var dumpFiles = Directory.GetFiles(dumpDir, "*.sql");

            if (!dumpFiles.Any())
            {
                throw new Exception("No SQL dump files found in the backup archive");
            }

            // Filter databases if specified
            if (databases != null && databases.Length > 0)
            {
                dumpFiles = dumpFiles.Where(f => databases.Contains(Path.GetFileNameWithoutExtension(f))).ToArray();
                if (!dumpFiles.Any())
                {
                    throw new Exception("No matching database dumps found for the specified databases");
                }
            }

            // Set environment variables for authentication
            var envVars = new Dictionary<string, string>
            {
                { "PGPASSWORD", _password }
            };

            var progressTask = new TaskCompletionSource<bool>();

            await AnsiConsole.Progress()
                .Columns(new ProgressColumn[]
                {
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn(),
                })
                .StartAsync(async ctx =>
                {
                    var overallTask = ctx.AddTask("[yellow]Restoring PostgreSQL dumps[/]");
                    overallTask.MaxValue = dumpFiles.Length;

                    foreach (var dumpFile in dumpFiles)
                    {
                        var dbName = Path.GetFileNameWithoutExtension(dumpFile);
                        var restoreTask = ctx.AddTask($"[yellow]Restoring database: {dbName}[/]");
                        restoreTask.MaxValue = 100;
                        restoreTask.StartTask();

                        // First, ensure we can connect to postgres to create/drop the database
                        var args = new List<string>
                        {
                            $"--host={_host}",
                            $"--port={_port}",
                            $"--username={_username}",
                            "--dbname=postgres",
                            $"--file={dumpFile}"
                        };

                        using var process = new Process
                        {
                            StartInfo = new ProcessStartInfo
                            {
                                FileName = "psql",
                                Arguments = string.Join(" ", args),
                                RedirectStandardOutput = true,
                                RedirectStandardError = true,
                                UseShellExecute = false,
                                CreateNoWindow = true
                            }
                        };

                        foreach (var envVar in envVars)
                        {
                            process.StartInfo.EnvironmentVariables[envVar.Key] = envVar.Value;
                        }

                        var outputBuilder = new System.Text.StringBuilder();
                        var errorBuilder = new System.Text.StringBuilder();

                        process.OutputDataReceived += (sender, e) =>
                        {
                            if (!string.IsNullOrEmpty(e.Data))
                            {
                                outputBuilder.AppendLine(e.Data);
                                restoreTask.Increment(1);
                            }
                        };

                        process.ErrorDataReceived += (sender, e) =>
                        {
                            if (!string.IsNullOrEmpty(e.Data))
                            {
                                errorBuilder.AppendLine(e.Data);
                            }
                        };

                        process.Start();
                        process.BeginOutputReadLine();
                        process.BeginErrorReadLine();
                        await process.WaitForExitAsync();

                        if (process.ExitCode != 0)
                        {
                            var error = errorBuilder.ToString();
                            throw new Exception($"PostgreSQL restore failed for database {dbName} with exit code {process.ExitCode}: {error}");
                        }

                        restoreTask.Value = restoreTask.MaxValue;
                        overallTask.Increment(1);

                        AnsiConsole.MarkupLine($"[green]Successfully restored database: {dbName}[/]");
                    }

                    overallTask.Value = overallTask.MaxValue;
                    progressTask.SetResult(true);
                });

            await progressTask.Task;
        }
        finally
        {
            // Clean up the temporary directory
            try
            {
                if (Directory.Exists(extractDir))
                {
                    Directory.Delete(extractDir, true);
                }
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[yellow]Warning: Could not delete temporary directory {extractDir}: {ex.Message}[/]");
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
