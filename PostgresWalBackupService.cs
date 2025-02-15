using System.Diagnostics;
using Amazon.S3;
using Amazon.S3.Model;
using Spectre.Console;

namespace BackupFlowCLI;

public class PostgresWalBackupService : IDisposable
{
    private readonly string _connectionString;
    private readonly string _host;
    private readonly int _port;
    private readonly string _username;
    private readonly string _password;
    private readonly string _s3Bucket;
    private readonly string _s3Prefix;
    private readonly S3Service _s3Service;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private Task? _backupTask;
    private readonly int _intervalMinutes;
    private readonly bool _useConnectionString;

    public PostgresWalBackupService(
        string host,
        int port,
        string username,
        string password,
        string s3Endpoint,
        string s3AccessKey,
        string s3SecretKey,
        string s3Bucket,
        string s3Prefix,
        int intervalMinutes = 1,
        bool useConnectionString = false)
    {
        _useConnectionString = useConnectionString;
        _host = host;
        _port = port;
        _username = username;
        _password = password;
        _connectionString = $"Host={host};Port={port};Username={username};Password={password};";
        _s3Bucket = s3Bucket;
        _s3Prefix = s3Prefix;
        _s3Service = new S3Service(s3Endpoint, s3AccessKey, s3SecretKey);
        _cancellationTokenSource = new CancellationTokenSource();
        _intervalMinutes = intervalMinutes;
    }

    public PostgresWalBackupService(
        string connectionString,
        string s3Endpoint,
        string s3AccessKey,
        string s3SecretKey,
        string s3Bucket,
        string s3Prefix,
        int intervalMinutes = 1,
        bool useConnectionString = false)
    {
        _useConnectionString = useConnectionString;
        _connectionString = connectionString;
        _s3Bucket = s3Bucket;
        _s3Prefix = s3Prefix;
        _s3Service = new S3Service(s3Endpoint, s3AccessKey, s3SecretKey);
        _cancellationTokenSource = new CancellationTokenSource();
        _intervalMinutes = intervalMinutes;

        // parse connection string 
        var csb = new Npgsql.NpgsqlConnectionStringBuilder(connectionString);
        _host = csb.Host ?? "localhost";
        _port = csb.Port;
        _username = csb.Username ?? "";
        _password = csb.Password ?? "";
    }

    public async Task StartContinuousBackup()
    {
        try
        {
            // First, ensure WAL archiving is enabled
            await EnableWalArchiving();

            // Start continuous WAL backup task
            _backupTask = Task.Run(async () =>
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    try
                    {
                        await BackupWalFiles();
                        await Task.Delay(TimeSpan.FromMinutes(_intervalMinutes), _cancellationTokenSource.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        AnsiConsole.MarkupLine($"[red]Error during WAL backup: {ex.Message}[/]");
                        await Task.Delay(TimeSpan.FromSeconds(30), _cancellationTokenSource.Token); // Wait before retrying
                    }
                }
            }, _cancellationTokenSource.Token);

            AnsiConsole.MarkupLine("[green]Continuous WAL archiving started[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Failed to start continuous WAL archiving: {ex.Message}[/]");
            throw;
        }
    }

    private async Task EnableWalArchiving()
    {
        // Create separate directories for WAL files and scripts
        var baseDir = Path.Combine(Path.GetTempPath(), "postgres_archive");
        var walDir = Path.Combine(baseDir, "wal");

        if (Directory.Exists(baseDir))
        {
            Directory.Delete(baseDir, true);
        }
        Directory.CreateDirectory(baseDir);
        Directory.CreateDirectory(walDir);

        // Ensure directories have proper permissions (777 for testing)
        var chmodProcess = Process.Start("chmod", $"-R 777 {baseDir}");
        await chmodProcess!.WaitForExitAsync();
        AnsiConsole.MarkupLine($"[blue]Created WAL archive directory with full permissions: {walDir}[/]");

        // Set environment variables for authentication
        var envVars = new Dictionary<string, string>
        {
            { "PGPASSWORD", _password }
        };

        // First, check current WAL settings
        var checkCommands = new[]
        {
            "SHOW wal_level;",
            "SHOW archive_mode;",
            "SHOW archive_command;",
            "SHOW archive_timeout;",
            "SELECT pg_is_in_recovery();",
            "SELECT current_setting('data_directory');",
            "SELECT current_setting('log_destination');",
            "SELECT current_setting('logging_collector');",
            "SELECT current_setting('log_directory');",
            "SELECT current_setting('log_filename');"
        };

        AnsiConsole.MarkupLine("[yellow]Current PostgreSQL WAL settings:[/]");
        foreach (var command in checkCommands)
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "psql",
                    Arguments = $"--host={_host} --port={_port} --username={_username} --dbname=postgres --tuples-only --command=\"{command}\"",
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

            process.Start();
            var output = await process.StandardOutput.ReadToEndAsync();
            var error = await process.StandardError.ReadToEndAsync();
            await process.WaitForExitAsync();

            if (!string.IsNullOrEmpty(error))
            {
                AnsiConsole.MarkupLine($"[red]Error executing {command}: {error}[/]");
            }
            AnsiConsole.MarkupLine($"[grey]{command.TrimEnd(';')}: {output.Trim()}[/]");
        }

        // Get PostgreSQL data directory
        string pgDataDir;
        using (var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "psql",
                Arguments = $"--host={_host} --port={_port} --username={_username} --dbname=postgres --tuples-only --command=\"SHOW data_directory;\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        })
        {
            foreach (var envVar in envVars)
            {
                process.StartInfo.EnvironmentVariables[envVar.Key] = envVar.Value;
            }

            process.Start();
            pgDataDir = (await process.StandardOutput.ReadToEndAsync()).Trim();
            await process.WaitForExitAsync();
        }

        AnsiConsole.MarkupLine($"[blue]PostgreSQL data directory: {pgDataDir}[/]");

        // Commands to enable WAL archiving with a simpler archive command
        var commands = new[]
        {
            "ALTER SYSTEM SET wal_level = 'replica';",
            "ALTER SYSTEM SET archive_mode = 'on';",
            $"ALTER SYSTEM SET archive_command = 'cp \"%p\" \"{walDir}/%f\"';",
            "ALTER SYSTEM SET archive_timeout = '60';",
            "ALTER SYSTEM SET max_wal_senders = '3';",
            "ALTER SYSTEM SET logging_collector = 'on';",
            "ALTER SYSTEM SET log_directory = 'pg_log';",
            "ALTER SYSTEM SET log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log';",
            "ALTER SYSTEM SET log_min_messages = 'debug1';",
            "SELECT pg_reload_conf();" // Reload the configuration
        };

        foreach (var command in commands)
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "psql",
                    Arguments = $"--host={_host} --port={_port} --username={_username} --dbname=postgres --command=\"{command}\"",
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
                throw new Exception($"Failed to execute command '{command}': {error}");
            }
        }

        AnsiConsole.MarkupLine("[green]WAL archiving configuration updated[/]");
        AnsiConsole.MarkupLine("[yellow]You must restart PostgreSQL for archive_mode changes to take effect.[/]");
        AnsiConsole.MarkupLine("[yellow]Please run: sudo systemctl restart postgresql[/]");

        // Wait for user to restart PostgreSQL
        AnsiConsole.MarkupLine("[yellow]Press Enter after restarting PostgreSQL...[/]");
        Console.ReadLine();

        // Force a log switch to test WAL archiving
        AnsiConsole.MarkupLine("[yellow]Testing WAL archiving with pg_switch_wal()...[/]");
        using (var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "psql",
                Arguments = $"--host={_host} --port={_port} --username={_username} --dbname=postgres --command=\"SELECT pg_switch_wal(), pg_walfile_name(pg_current_wal_lsn());\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        })
        {
            foreach (var envVar in envVars)
            {
                process.StartInfo.EnvironmentVariables[envVar.Key] = envVar.Value;
            }

            process.Start();
            var output = await process.StandardOutput.ReadToEndAsync();
            var error = await process.StandardError.ReadToEndAsync();
            await process.WaitForExitAsync();

            if (!string.IsNullOrEmpty(error))
            {
                AnsiConsole.MarkupLine($"[red]Error switching WAL: {error}[/]");
            }
            else
            {
                AnsiConsole.MarkupLine($"[green]WAL switch successful: {output.Trim()}[/]");
            }
        }

        // Check PostgreSQL logs for any errors
        var logPath = Path.Combine(pgDataDir, "pg_log");
        if (Directory.Exists(logPath))
        {
            var logFiles = Directory.GetFiles(logPath, "postgresql-*.log")
                .OrderByDescending(f => File.GetLastWriteTime(f))
                .Take(1)
                .ToList();

            if (logFiles.Any())
            {
                var latestLog = logFiles.First();
                AnsiConsole.MarkupLine($"[yellow]Checking PostgreSQL log file: {latestLog}[/]");
                var logContent = await File.ReadAllLinesAsync(latestLog);
                var recentLogs = logContent.TakeLast(20); // Show last 20 lines
                foreach (var line in recentLogs)
                {
                    AnsiConsole.MarkupLine($"[grey]{line}[/]");
                }
            }
        }

        // Check if WAL directory is receiving files
        await Task.Delay(2000); // Wait a bit for the WAL file to be archived
        var walFiles = Directory.GetFiles(walDir).Where(f => Path.GetFileName(f).Length == 24 && Path.GetFileName(f).All(c => char.IsLetterOrDigit(c))).ToArray();
        if (walFiles.Length > 0)
        {
            AnsiConsole.MarkupLine($"[green]WAL archiving is working! Found {walFiles.Length} WAL file(s) in {walDir}[/]");
            foreach (var file in walFiles)
            {
                AnsiConsole.MarkupLine($"[grey]Found WAL file: {Path.GetFileName(file)}[/]");
            }
        }
        else
        {
            AnsiConsole.MarkupLine($"[red]Warning: No WAL files found in {walDir} after forcing log switch[/]");
            AnsiConsole.MarkupLine("[yellow]Please check that PostgreSQL has been restarted and has proper permissions[/]");
        }
    }

    private async Task BackupWalFiles()
    {
        var walDir = Path.Combine(Path.GetTempPath(), "postgres_archive", "wal");
        // Only look for WAL files (they have a specific naming pattern)
        var walFiles = Directory.GetFiles(walDir)
            .Where(f => Path.GetFileName(f).Length == 24 && Path.GetFileName(f).All(c => char.IsLetterOrDigit(c)))
            .ToArray();

        if (walFiles.Length == 0)
        {
            AnsiConsole.MarkupLine($"[grey]No WAL files found in {walDir}[/]");
            return;
        }

        AnsiConsole.MarkupLine($"[blue]Found {walFiles.Length} WAL file(s) to process[/]");

        foreach (var walFile in walFiles)
        {
            var fileName = Path.GetFileName(walFile);
            var s3Key = $"{_s3Prefix.TrimEnd('/')}/wals/{fileName}";

            try
            {
                // Upload WAL file to S3
                await using var fileStream = File.OpenRead(walFile);
                var putObjectRequest = new PutObjectRequest
                {
                    BucketName = _s3Bucket,
                    Key = s3Key,
                    InputStream = fileStream
                };

                await _s3Service._s3Client.PutObjectAsync(putObjectRequest);
                AnsiConsole.MarkupLine($"[green]Uploaded WAL file: {fileName}[/]");

                // Delete the local WAL file after successful upload
                File.Delete(walFile);
                AnsiConsole.MarkupLine($"[grey]Deleted local WAL file: {fileName}[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Error uploading WAL file {fileName}: {ex.Message}[/]");
                throw;
            }
        }
    }

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();
        _backupTask?.Wait();
        _cancellationTokenSource.Dispose();
    }
}
