using System.Diagnostics;
using MongoDB.Driver;
using Spectre.Console;

namespace DbBackupCLI;

public class MongoBackupService
{
    private readonly string _connectionString;
    private readonly string _username;
    private readonly string _password;
    private readonly string _host;
    private readonly int _port;

    public MongoBackupService(string host, int port, string username, string password)
    {
        _host = host;
        _port = port;
        _username = username;
        _password = password;
        _connectionString = $"mongodb://{username}:{password}@{host}:{port}";
    }

    private async Task<bool> IsReplicaSetMember()
    {
        try
        {
            var client = new MongoClient(_connectionString);
            var db = client.GetDatabase("admin");
            var result = await db.RunCommandAsync<dynamic>(new MongoDB.Bson.BsonDocument("isMaster", 1));
            return result.setName != null;
        }
        catch
        {
            return false;
        }
    }

    public async Task<(string outputPath, DateTime timestamp)> CreateBackup(string[] databases, bool includeOplog = false)
    {
        var timestamp = DateTime.UtcNow;
        var outputPath = Path.Combine(Path.GetTempPath(), $"mongodb_backup_{timestamp:yyyyMMdd_HHmmss}.archive.gz");

        var args = new List<string>
        {
            "--archive=" + outputPath,
            "--gzip",
            $"--host={_host}",
            $"--port={_port}",
            $"--username={_username}",
            $"--password={_password}",
            "--authenticationDatabase=admin"
        };

        if (includeOplog)
        {
            if (await IsReplicaSetMember())
            {
                args.Add("--oplog");
                AnsiConsole.MarkupLine("[blue]Including oplog in backup for point-in-time recovery[/]");
            }
            else
            {
                AnsiConsole.MarkupLine("[yellow]Warning: Oplog backup requested but the target MongoDB instance is not a replica set member. Continuing without oplog backup.[/]");
            }
        }

        if (databases != null && databases.Length > 0)
        {
            args.AddRange(databases.Select(db => $"--db={db}"));
        }

        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "mongodump",
                Arguments = string.Join(" ", args),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        var outputBuilder = new System.Text.StringBuilder();
        var errorBuilder = new System.Text.StringBuilder();

        process.OutputDataReceived += (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                outputBuilder.AppendLine(e.Data);
                AnsiConsole.MarkupLine($"[grey]{e.Data}[/]");
            }
        };

        process.ErrorDataReceived += (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                errorBuilder.AppendLine(e.Data);
                // Escape any special characters that could be interpreted as markup
                var escapedData = e.Data.Replace("[", "[[").Replace("]", "]]");
                AnsiConsole.MarkupLine($"[yellow]{escapedData}[/]");
            }
        };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        await process.WaitForExitAsync();

        if (process.ExitCode != 0)
        {
            var error = errorBuilder.ToString();
            throw new Exception($"MongoDB backup failed with exit code {process.ExitCode}: {error}");
        }

        return (outputPath, timestamp);
    }
}
