using System.Diagnostics;
using Spectre.Console;

namespace DbBackupCLI;

public class PostgresBackupService
{
    private readonly string _host;
    private readonly int _port;
    private readonly string _username;
    private readonly string _password;
    private readonly string? _connectionString;

    public PostgresBackupService(string host, int port, string username, string password)
    {
        _host = host;
        _port = port;
        _username = username;
        _password = password;
        _connectionString = null;
    }

    public PostgresBackupService(string connectionString)
    {
        _connectionString = connectionString;

        // Parse connection string to get individual components
        var csb = new Npgsql.NpgsqlConnectionStringBuilder(connectionString);
        _host = csb.Host ?? "localhost";
        _port = csb.Port;
        _username = csb.Username ?? "";
        _password = csb.Password ?? "";
    }

    public async Task<(string outputPath, DateTime timestamp)> CreateBackup()
    {
        var timestamp = DateTime.UtcNow;
        var outputDir = Path.Combine(Path.GetTempPath(), $"postgres_backup_{timestamp:yyyyMMdd_HHmmss}");
        Directory.CreateDirectory(outputDir);

        // Set environment variables for authentication
        var envVars = new Dictionary<string, string>
        {
            { "PGPASSWORD", _password }
        };

        var args = new List<string>
        {
            $"--host={_host}",
            $"--port={_port}",
            $"--username={_username}",
            "--format=tar",
            "--wal-method=fetch",
            "--progress",
            "--verbose",
            $"--pgdata={outputDir}"
        };

        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "pg_basebackup",
                Arguments = string.Join(" ", args),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        // Add environment variables
        foreach (var envVar in envVars)
        {
            process.StartInfo.EnvironmentVariables[envVar.Key] = envVar.Value;
        }

        var outputBuilder = new System.Text.StringBuilder();
        var errorBuilder = new System.Text.StringBuilder();
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
                var backupTask = ctx.AddTask("[yellow]Creating PostgreSQL backup[/]");
                backupTask.MaxValue = 100;
                backupTask.StartTask();

                process.OutputDataReceived += (sender, e) =>
                {
                    if (!string.IsNullOrEmpty(e.Data))
                    {
                        outputBuilder.AppendLine(e.Data);
                        if (e.Data.Contains("%"))
                        {
                            try
                            {
                                var percentStr = e.Data.Split('%')[0].Split(' ').Last();
                                if (double.TryParse(percentStr, out double percent))
                                {
                                    backupTask.Value = percent;
                                }
                            }
                            catch
                            {
                                // Ignore parsing errors
                            }
                        }
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
                backupTask.Value = 100;
                progressTask.SetResult(true);
            });

        await progressTask.Task;

        if (process.ExitCode != 0)
        {
            var error = errorBuilder.ToString();
            Directory.Delete(outputDir, true);
            throw new Exception($"PostgreSQL backup failed with exit code {process.ExitCode}: {error}");
        }

        // Create a tar.gz archive of the backup directory
        var archivePath = $"{outputDir}.tar.gz";
        await CreateTarGzArchive(outputDir, archivePath);

        // Clean up the temporary directory
        Directory.Delete(outputDir, true);

        return (archivePath, timestamp);
    }

    private async Task CreateTarGzArchive(string sourceDir, string archivePath)
    {
        var args = $"-czf \"{archivePath}\" -C \"{Path.GetDirectoryName(sourceDir)}\" \"{Path.GetFileName(sourceDir)}\"";

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
            throw new Exception($"Failed to create tar.gz archive: {error}");
        }
    }
}
