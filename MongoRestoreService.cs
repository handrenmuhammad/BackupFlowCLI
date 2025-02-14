using System.Diagnostics;
using MongoDB.Driver;
using Spectre.Console;

namespace DbBackupCLI;

public class MongoRestoreService
{
    private readonly string _connectionString;
    private readonly string _username;
    private readonly string _password;
    private readonly string _host;
    private readonly int _port;

    public MongoRestoreService(string host, int port, string username, string password)
    {
        _host = host;
        _port = port;
        _username = username;
        _password = password;
        _connectionString = $"mongodb://{username}:{password}@{host}:{port}/?authSource=admin&directConnection=true";
    }

    public MongoRestoreService(string connectionString)
    {
        _connectionString = connectionString;
    }

    private async Task<bool> IsReplicaSetMember()
    {
        try
        {
            var client = new MongoClient(_connectionString);
            var db = client.GetDatabase("admin");
            var command = new MongoDB.Bson.BsonDocument("isMaster", 1);
            var result = await db.RunCommandAsync<MongoDB.Bson.BsonDocument>(command);

            bool isReplicaSet = result.Contains("setName") ||
                               (result.Contains("isreplicaset") && result["isreplicaset"].AsBoolean) ||
                               (result.Contains("hosts") && result["hosts"].AsBsonArray.Count > 0);

            if (isReplicaSet)
            {
                AnsiConsole.MarkupLine("[blue]Detected as replica set member[/]");
            }
            else
            {
                AnsiConsole.MarkupLine("[yellow]Not detected as replica set member[/]");
            }

            return isReplicaSet;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error checking replica set status: {ex.Message}[/]");
            return false;
        }
    }

    public async Task RestoreBackup(string archivePath, string[]? databases = null, bool includeOplog = false)
    {
        var args = new List<string>();

        // If we have a direct connection string, use --uri, otherwise use individual parameters
        if (string.IsNullOrEmpty(_host))
        {
            args.Add($"--uri=\"{_connectionString}\"");
        }
        else
        {
            args.AddRange(
            [
                $"--host={_host}",
                $"--port={_port}",
                $"--username={_username}",
                $"--password={_password}",
                "--authenticationDatabase=admin"
            ]);
        }

        // Check if we're trying to use both oplog replay and database filtering
        if (includeOplog && databases != null && databases.Length > 0)
        {
            AnsiConsole.MarkupLine("[yellow]Warning: Cannot use oplog replay with specific database restore. Oplog replay will be disabled.[/]");
            includeOplog = false;
        }

        if (includeOplog)
        {
            if (await IsReplicaSetMember())
            {
                args.Add("--oplogReplay");
                AnsiConsole.MarkupLine("[blue]Including oplog replay for point-in-time recovery[/]");
            }
            else
            {
                AnsiConsole.MarkupLine("[yellow]Warning: Oplog replay requested but the target MongoDB instance is not a replica set member. Continuing without oplog replay.[/]");
            }
        }

        // Add database filtering if specified
        if (databases != null && databases.Length > 0)
        {
            if (databases.Length == 1)
            {
                // For single database restore, use --db
                args.Add($"--db={databases[0]}");
                AnsiConsole.MarkupLine($"[blue]Restoring single database: {databases[0]}[/]");
            }
            else
            {
                // For multiple databases, use --nsInclude
                foreach (var db in databases)
                {
                    args.Add($"--nsInclude={db}.*");
                }
                AnsiConsole.MarkupLine($"[blue]Restoring multiple databases: {string.Join(", ", databases)}[/]");
            }
        }

        // Add archive options at the end
        args.Add("--gzip");
        args.Add($"--archive={archivePath}");

        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "mongorestore",
                Arguments = string.Join(" ", args),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        AnsiConsole.MarkupLine($"[grey]Running command: mongorestore {process.StartInfo.Arguments}[/]");

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
                var restoreTask = ctx.AddTask("[yellow]Restoring database[/]");
                restoreTask.MaxValue = 100;
                restoreTask.StartTask();

                process.OutputDataReceived += (sender, e) =>
                {
                    if (!string.IsNullOrEmpty(e.Data))
                    {
                        outputBuilder.AppendLine(e.Data);
                        AnsiConsole.MarkupLine($"[grey]{e.Data}[/]");

                        // Update progress based on output
                        if (e.Data.Contains("restoring indexes"))
                        {
                            restoreTask.Value = 80;
                        }
                        else if (e.Data.Contains("done"))
                        {
                            restoreTask.Value = 95;
                        }
                        else
                        {
                            restoreTask.Increment(1);
                        }
                    }
                };

                process.ErrorDataReceived += (sender, e) =>
                {
                    if (!string.IsNullOrEmpty(e.Data))
                    {
                        errorBuilder.AppendLine(e.Data);
                        var escapedData = e.Data.Replace("[", "[[").Replace("]", "]]");
                        AnsiConsole.MarkupLine($"[yellow]{escapedData}[/]");
                    }
                };

                process.Start();
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();
                await process.WaitForExitAsync();
                restoreTask.Value = 100;
                progressTask.SetResult(true);
            });

        await progressTask.Task;

        if (process.ExitCode != 0)
        {
            var error = errorBuilder.ToString();
            throw new Exception($"MongoDB restore failed with exit code {process.ExitCode}: {error}");
        }
    }

    public async Task RestoreOplog(string archivePath)
    {
        if (!await IsReplicaSetMember())
        {
            throw new InvalidOperationException("Oplog replay requires a replica set member");
        }

        var args = new List<string>();

        // Add connection parameters
        if (string.IsNullOrEmpty(_host))
        {
            args.Add($"--uri=\"{_connectionString}\"");
        }
        else
        {
            args.AddRange(
            [
                $"--host={_host}",
                $"--port={_port}",
                $"--username={_username}",
                $"--password={_password}",
                "--authenticationDatabase=admin"
            ]);
        }

        // Add restore options
        args.AddRange(
        [
            "--gzip",
            $"--archive={archivePath}",
            "--oplogReplay",
            "--noIndexRestore", // Skip index rebuilding during oplog replay
            "--quiet"
        ]);

        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "mongorestore",
                Arguments = string.Join(" ", args),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        AnsiConsole.MarkupLine($"[grey]Applying oplog: mongorestore {process.StartInfo.Arguments}[/]");

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
                var oplogTask = ctx.AddTask("[yellow]Replaying oplog entries[/]");
                oplogTask.MaxValue = 100;
                oplogTask.StartTask();

                process.OutputDataReceived += (sender, e) =>
                {
                    if (!string.IsNullOrEmpty(e.Data))
                    {
                        outputBuilder.AppendLine(e.Data);
                        AnsiConsole.MarkupLine($"[grey]{e.Data}[/]");

                        // Update progress based on output
                        if (e.Data.Contains("replaying operations"))
                        {
                            oplogTask.Value = 50;
                        }
                        else if (e.Data.Contains("applied ops"))
                        {
                            oplogTask.Value = 90;
                        }
                        else
                        {
                            oplogTask.Increment(1);
                        }
                    }
                };

                process.ErrorDataReceived += (sender, e) =>
                {
                    if (!string.IsNullOrEmpty(e.Data))
                    {
                        errorBuilder.AppendLine(e.Data);
                        var escapedData = e.Data.Replace("[", "[[").Replace("]", "]]");
                        AnsiConsole.MarkupLine($"[yellow]{escapedData}[/]");
                    }
                };

                process.Start();
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();
                await process.WaitForExitAsync();
                oplogTask.Value = 100;
                progressTask.SetResult(true);
            });

        await progressTask.Task;

        var output = outputBuilder.ToString();
        var error = errorBuilder.ToString();

        if (process.ExitCode != 0)
        {
            throw new Exception($"Failed to apply oplog. Exit code: {process.ExitCode}\nOutput: {output}\nError: {error}");
        }
    }
}
