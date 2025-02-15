using System.Diagnostics;
using MongoDB.Driver;
using Spectre.Console;

namespace BackupFlowCLI;

public class MongoOplogBackupService : IDisposable
{
    private readonly string _connectionString;
    private readonly string _username;
    private readonly string _password;
    private readonly string _host;
    private readonly int _port;
    private readonly string _s3Bucket;
    private readonly string _s3Prefix;
    private readonly S3Service _s3Service;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private Task? _backupTask;
    private readonly int _intervalMinutes;
    private readonly bool _useConnectionString;

    public MongoOplogBackupService(
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
        _connectionString = $"mongodb://{username}:{password}@{host}:{port}/?authSource=admin&directConnection=true";
        _s3Bucket = s3Bucket;
        _s3Prefix = s3Prefix;
        _s3Service = new S3Service(s3Endpoint, s3AccessKey, s3SecretKey);
        _cancellationTokenSource = new CancellationTokenSource();
        _intervalMinutes = intervalMinutes;
    }

    public MongoOplogBackupService(
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

            return isReplicaSet;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error checking replica set status: {ex.Message}[/]");
            return false;
        }
    }

    public async Task StartContinuousBackup()
    {
        if (!await IsReplicaSetMember())
        {
            throw new InvalidOperationException("Continuous oplog backup requires a replica set");
        }

        _backupTask = Task.Run(async () =>
        {
            while (!_cancellationTokenSource.Token.IsCancellationRequested)
            {
                try
                {
                    await BackupOplog();
                    await Task.Delay(TimeSpan.FromMinutes(_intervalMinutes), _cancellationTokenSource.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    AnsiConsole.MarkupLine($"[red]Error in oplog backup: {ex.Message}[/]");
                    // Wait a bit before retrying
                    await Task.Delay(TimeSpan.FromSeconds(30), _cancellationTokenSource.Token);
                }
            }
        });

        AnsiConsole.MarkupLine($"[green]Started continuous oplog backup (every {_intervalMinutes} minutes)[/]");
    }

    private async Task BackupOplog()
    {
        var timestamp = DateTime.UtcNow;
        var outputPath = Path.Combine(Path.GetTempPath(), $"oplog_backup_{timestamp:yyyyMMdd_HHmmss}.archive.gz");

        try
        {
            var args = new List<string>();

            // Connection parameters
            if (_useConnectionString)
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

            // Add oplog specific options
            args.AddRange(
            [
                "--archive=" + outputPath,
                "--gzip",
                "--oplog",
                "--quiet"
            ]);

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
                    var backupTask = ctx.AddTask("[yellow]Creating oplog backup[/]");
                    backupTask.MaxValue = 100;
                    backupTask.StartTask();

                    process.OutputDataReceived += (sender, e) =>
                    {
                        if (!string.IsNullOrEmpty(e.Data))
                        {
                            outputBuilder.AppendLine(e.Data);
                            // Increment progress based on output
                            backupTask.Increment(2);
                        }
                    };

                    process.ErrorDataReceived += (sender, e) =>
                    {
                        if (!string.IsNullOrEmpty(e.Data))
                        {
                            errorBuilder.AppendLine(e.Data);
                            if (e.Data.Contains("done dumping"))
                            {
                                backupTask.Value = 90;
                            }
                        }
                    };

                    process.Start();
                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();

                    await process.WaitForExitAsync(_cancellationTokenSource.Token);
                    backupTask.Value = 100;
                    progressTask.SetResult(true);
                });

            await progressTask.Task;

            if (process.ExitCode != 0)
            {
                throw new Exception($"Oplog backup failed: {errorBuilder}");
            }

            // Upload to S3
            var s3Key = $"{_s3Prefix.TrimEnd('/')}/oplogs/oplog_backup_{timestamp:yyyyMMdd_HHmmss}.archive.gz";
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
                    var uploadTask = ctx.AddTask("[yellow]Uploading to S3[/]");
                    uploadTask.MaxValue = 100;
                    uploadTask.StartTask();

                    await using (var fileStream = File.OpenRead(outputPath))
                    {
                        var putObjectRequest = new Amazon.S3.Model.PutObjectRequest
                        {
                            BucketName = _s3Bucket,
                            Key = s3Key,
                            InputStream = fileStream
                        };

                        putObjectRequest.StreamTransferProgress += (sender, args) =>
                        {
                            var percentDone = (double)args.TransferredBytes / args.TotalBytes * 100;
                            uploadTask.Value = percentDone;
                        };

                        await _s3Service._s3Client.PutObjectAsync(putObjectRequest, _cancellationTokenSource.Token);
                    }
                });

            AnsiConsole.MarkupLine($"[green]Oplog backup completed and uploaded to S3: {s3Key}[/]");
        }
        finally
        {
            if (File.Exists(outputPath))
            {
                try
                {
                    File.Delete(outputPath);
                }
                catch
                {
                }
            }
        }
    }

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();
        if (_backupTask != null)
        {
            try
            {
                _backupTask.Wait(TimeSpan.FromSeconds(30));
            }
            catch
            {
            }
        }
        _cancellationTokenSource.Dispose();
    }
}
