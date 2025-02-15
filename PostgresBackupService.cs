using System.Diagnostics;
using Spectre.Console;

namespace BackupFlowCLI;

public enum PostgresBackupType
{
    BaseBackup,
    Dump
}

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

    public async Task<(string outputPath, DateTime timestamp)> CreateBackup(
        PostgresBackupType backupType = PostgresBackupType.BaseBackup,
        bool includeWal = true,
        string[]? databases = null)
    {
        var timestamp = DateTime.UtcNow;

        switch (backupType)
        {
            case PostgresBackupType.BaseBackup:
                return await CreateBaseBackup(timestamp, includeWal);
            case PostgresBackupType.Dump:
                return await CreateDump(timestamp, databases);
            default:
                throw new ArgumentException($"Unsupported backup type: {backupType}");
        }
    }

    private async Task<(string outputPath, DateTime timestamp)> CreateBaseBackup(DateTime timestamp, bool includeWal)
    {
        // Create a descriptive directory name
        var outputDir = Path.Combine(Path.GetTempPath(), $"postgres_basebackup_{timestamp:yyyyMMdd_HHmmss}");
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
            "--format=plain",
            "--verbose",
            $"--pgdata={outputDir}"
        };

        // Add WAL options if requested
        if (includeWal)
        {
            args.AddRange(new[]
            {
                "--wal-method=fetch", // Fetch WAL files during backup
                "--checkpoint=fast"   // Force a fast checkpoint before backup
            });
        }

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
                var backupTask = ctx.AddTask("[yellow]Creating PostgreSQL base backup[/]");
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
            throw new Exception($"PostgreSQL base backup failed with exit code {process.ExitCode}: {error}");
        }

        // Create a tar.gz archive of the backup directory
        var archivePath = $"{outputDir}.tar.gz";
        await CreateTarGzArchive(outputDir, archivePath);

        // Clean up the temporary directory
        Directory.Delete(outputDir, true);

        return (archivePath, timestamp);
    }

    private async Task<(string outputPath, DateTime timestamp)> CreateDump(DateTime timestamp, string[]? databases)
    {
        // Create a descriptive directory name that includes database info
        var dirNameParts = new List<string> { "postgres_dump", timestamp.ToString("yyyyMMdd_HHmmss") };
        if (databases?.Length == 1)
        {
            dirNameParts.Add(databases[0]); // Add the single database name to the directory
        }
        var outputDir = Path.Combine(Path.GetTempPath(), string.Join("_", dirNameParts));
        Directory.CreateDirectory(outputDir);

        // Set environment variables for authentication
        var envVars = new Dictionary<string, string>
        {
            { "PGPASSWORD", _password }
        };

        // Get list of databases if not specified
        if (databases == null || databases.Length == 0)
        {
            databases = await GetDatabases();
            AnsiConsole.MarkupLine($"[blue]Found databases: {string.Join(", ", databases)}[/]");
        }

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
                var overallTask = ctx.AddTask("[yellow]Creating PostgreSQL dumps[/]");
                overallTask.MaxValue = databases.Length;

                foreach (var database in databases)
                {
                    var dumpTask = ctx.AddTask($"[yellow]Dumping database: {database}[/]");
                    dumpTask.MaxValue = 100;
                    dumpTask.StartTask();

                    var outputFile = Path.Combine(outputDir, $"{database}.sql");
                    var args = new List<string>
                    {
                        $"--host={_host}",
                        $"--port={_port}",
                        $"--username={_username}",
                        "--format=plain",
                        "--verbose",
                        "--create",
                        "--clean",
                        "--if-exists",
                        $"--file={outputFile}",
                        database
                    };

                    using var process = new Process
                    {
                        StartInfo = new ProcessStartInfo
                        {
                            FileName = "pg_dump",
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
                            dumpTask.Increment(1);
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
                        Directory.Delete(outputDir, true);
                        throw new Exception($"PostgreSQL dump failed for database {database} with exit code {process.ExitCode}: {error}");
                    }

                    dumpTask.Value = dumpTask.MaxValue;
                    overallTask.Increment(1);
                }

                overallTask.Value = overallTask.MaxValue;
                progressTask.SetResult(true);
            });

        await progressTask.Task;

        // Create a tar.gz archive of the dump directory
        var archiveName = databases.Length == 1
            ? $"postgres_dump_{databases[0]}_{timestamp:yyyyMMdd_HHmmss}.tar.gz"
            : $"postgres_dump_{timestamp:yyyyMMdd_HHmmss}.tar.gz";
        var archivePath = Path.Combine(Path.GetDirectoryName(outputDir)!, archiveName);
        await CreateTarGzArchive(outputDir, archivePath);

        // Clean up the temporary directory
        Directory.Delete(outputDir, true);

        return (archivePath, timestamp);
    }

    private async Task<string[]> GetDatabases()
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "psql",
                Arguments = $"--host={_host} --port={_port} --username={_username} --tuples-only --command=\"SELECT datname FROM pg_database WHERE datistemplate = false AND datname != 'postgres';\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        process.StartInfo.EnvironmentVariables["PGPASSWORD"] = _password;

        process.Start();
        var output = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();

        if (process.ExitCode != 0)
        {
            throw new Exception("Failed to get list of databases");
        }

        return output.Split('\n')
            .Where(line => !string.IsNullOrWhiteSpace(line))
            .Select(line => line.Trim())
            .ToArray();
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
