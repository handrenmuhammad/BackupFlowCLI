using System.CommandLine;
using System.CommandLine.IO;
using System.Diagnostics;
using System.Text;
using System.Runtime.InteropServices;
using Amazon.S3;
using Amazon.S3.Model;
using DbBackupCLI;
using Spectre.Console;

public interface IBackupAnalyzer
{
    Task<(string? timestamp, HashSet<string> databases)> AnalyzeBackup(string archivePath);
}

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        var rootCommand = new RootCommand("DbBackupCLI");
        var backupCommand = new Command("backup", "Creates a backup of the specified database and uploads it to S3");
        var restoreCommand = new Command("restore", "Restores a backup of the specified database from S3");
        var listCommand = new Command("list", @"Lists all database backups from S3.
        Examples:
          dbbackup list --type=mongodb --s3-endpoint=s3.amazonaws.com --s3-access-key=YOUR_ACCESS_KEY --s3-secret-key=YOUR_SECRET_KEY --s3-bucket=my-backup-bucket
          dbbackup list --type=postgresql --s3-endpoint=s3.amazonaws.com --s3-access-key=YOUR_ACCESS_KEY --s3-secret-key=YOUR_SECRET_KEY --s3-bucket=my-backup-bucket --prefix=prod/
          dbbackup list --type=mongodb --latest-only --s3-endpoint=s3.amazonaws.com --s3-access-key=YOUR_ACCESS_KEY --s3-secret-key=YOUR_SECRET_KEY --s3-bucket=my-backup-bucket
        ");

        var dbTypeOption = new Option<string>(
            "--type",
            description: "Database type (mongodb or postgresql)",
            getDefaultValue: () => "mongodb"
        );
        dbTypeOption.AddAlias("-t");

        var hostOption = new Option<string>(
            "--host",
            description: "Database host",
            getDefaultValue: () => "localhost"
        );

        var portOption = new Option<int>(
            "--port",
            description: "Database port",
            getDefaultValue: () => 27017
        );

        var usernameOption = new Option<string>(
            "--username",
            description: "Database username"
        )
        { IsRequired = true };

        var passwordOption = new Option<string>(
            "--password",
            description: "Database password"
        )
        { IsRequired = true };

        var databasesOption = new Option<string[]>(
            "--databases",
            description: "Specific databases to backup (if not specified, all databases will be backed up)"
        )
        { AllowMultipleArgumentsPerToken = true };

        var includeOplogOption = new Option<bool>(
            "--oplog",
            description: "Include oplog in the backup for point-in-time recovery (MongoDB only)",
            getDefaultValue: () => false
        );

        var s3EndpointOption = new Option<string>("--s3-endpoint", description: "S3 endpoint (e.g., 's3.amazonaws.com' or your custom endpoint)") { IsRequired = true };
        var s3AccessKeyOption = new Option<string>("--s3-access-key", description: "AWS Access Key ID or compatible S3 access key") { IsRequired = true };
        var s3SecretKeyOption = new Option<string>("--s3-secret-key", description: "AWS Secret Access Key or compatible S3 secret key") { IsRequired = true };
        var s3BucketOption = new Option<string>("--s3-bucket", description: "The S3 bucket name containing the backups") { IsRequired = true };
        var prefixOption = new Option<string>("--prefix", description: "Optional prefix to filter backups (e.g., 'prod/' or 'dev/')") { IsRequired = false };
        var latestOnlyOption = new Option<bool>("--latest-only", description: "Show only the latest backup's database names", getDefaultValue: () => false);

        // Add options to commands
        backupCommand.AddOption(dbTypeOption);
        backupCommand.AddOption(hostOption);
        backupCommand.AddOption(portOption);
        backupCommand.AddOption(usernameOption);
        backupCommand.AddOption(passwordOption);
        backupCommand.AddOption(databasesOption);
        backupCommand.AddOption(includeOplogOption);
        backupCommand.AddOption(s3EndpointOption);
        backupCommand.AddOption(s3AccessKeyOption);
        backupCommand.AddOption(s3SecretKeyOption);
        backupCommand.AddOption(s3BucketOption);
        backupCommand.AddOption(prefixOption);

        listCommand.AddOption(dbTypeOption);
        listCommand.AddOption(latestOnlyOption);
        listCommand.AddOption(s3EndpointOption);
        listCommand.AddOption(s3AccessKeyOption);
        listCommand.AddOption(s3SecretKeyOption);
        listCommand.AddOption(s3BucketOption);
        listCommand.AddOption(prefixOption);

        rootCommand.AddCommand(backupCommand);
        rootCommand.AddCommand(restoreCommand);
        rootCommand.AddCommand(listCommand);

        backupCommand.SetHandler(async (context) =>
        {
            var dbType = context.ParseResult.GetValueForOption<string>(dbTypeOption)!;
            var host = context.ParseResult.GetValueForOption<string>(hostOption)!;
            var port = context.ParseResult.GetValueForOption<int>(portOption);
            var username = context.ParseResult.GetValueForOption<string>(usernameOption)!;
            var password = context.ParseResult.GetValueForOption<string>(passwordOption)!;
            var databases = context.ParseResult.GetValueForOption<string[]>(databasesOption);
            var includeOplog = context.ParseResult.GetValueForOption<bool>(includeOplogOption);
            var s3Endpoint = context.ParseResult.GetValueForOption<string>(s3EndpointOption)!;
            var s3AccessKey = context.ParseResult.GetValueForOption<string>(s3AccessKeyOption)!;
            var s3SecretKey = context.ParseResult.GetValueForOption<string>(s3SecretKeyOption)!;
            var s3Bucket = context.ParseResult.GetValueForOption<string>(s3BucketOption)!;
            var prefix = context.ParseResult.GetValueForOption<string>(prefixOption)!;

            await CreateBackup(context.Console, dbType, host, port, username, password, databases, includeOplog, s3Endpoint, s3AccessKey, s3SecretKey, s3Bucket, prefix);
        });

        listCommand.SetHandler(async (context) =>
        {
            var dbType = context.ParseResult.GetValueForOption<string>(dbTypeOption)!;
            var latestOnly = context.ParseResult.GetValueForOption<bool>(latestOnlyOption);
            var s3Endpoint = context.ParseResult.GetValueForOption<string>(s3EndpointOption)!;
            var s3AccessKey = context.ParseResult.GetValueForOption<string>(s3AccessKeyOption)!;
            var s3SecretKey = context.ParseResult.GetValueForOption<string>(s3SecretKeyOption)!;
            var s3Bucket = context.ParseResult.GetValueForOption<string>(s3BucketOption)!;
            var prefix = context.ParseResult.GetValueForOption<string>(prefixOption)!;
            await ListBackups(context.Console, dbType, latestOnly, s3Endpoint, s3AccessKey, s3SecretKey, s3Bucket, prefix);
        });

        return await rootCommand.InvokeAsync(args);
    }

    public static async Task CreateBackup(IConsole console, string dbType, string host, int port, string username, string password, string[]? databases, bool includeOplog, string s3Endpoint, string s3AccessKey, string s3SecretKey, string s3Bucket, string prefix)
    {
        try
        {
            AnsiConsole.MarkupLine($"[yellow]Creating {dbType} backup...[/]");

            string backupPath;
            DateTime timestamp;

            switch (dbType.ToLower())
            {
                case "mongodb":
                    var mongoBackupService = new MongoBackupService(host, port, username, password);
                    (backupPath, timestamp) = await mongoBackupService.CreateBackup(databases ?? Array.Empty<string>(), includeOplog);
                    break;
                default:
                    throw new ArgumentException($"Unsupported database type: {dbType}");
            }

            AnsiConsole.MarkupLine($"[green]Backup created successfully at: {backupPath}[/]");

            // Upload to S3
            AnsiConsole.MarkupLine("[yellow]Uploading backup to S3...[/]");
            var s3Service = new S3Service(s3Endpoint, s3AccessKey, s3SecretKey);

            // Ensure bucket exists
            await s3Service.EnsureBucketExistsAsync(s3Bucket);

            var s3Key = $"{prefix?.TrimEnd('/')}/{Path.GetFileName(backupPath)}";

            await using (var fileStream = File.OpenRead(backupPath))
            {
                var putObjectRequest = new PutObjectRequest
                {
                    BucketName = s3Bucket,
                    Key = s3Key,
                    InputStream = fileStream
                };

                await s3Service._s3Client.PutObjectAsync(putObjectRequest);
            }

            AnsiConsole.MarkupLine($"[green]Backup uploaded successfully to S3: {s3Key}[/]");

            // Cleanup temporary file
            try
            {
                File.Delete(backupPath);
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[yellow]Warning: Could not delete temporary file {backupPath}: {ex.Message}[/]");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error creating backup: {ex.Message}[/]");
            Environment.Exit(1);
        }
    }

    public static async Task ListBackups(IConsole console, string dbType, bool latestOnly, string s3Endpoint, string s3AccessKey, string s3SecretKey, string s3Bucket, string prefix)
    {
        try
        {
            var s3Service = new S3Service(s3Endpoint, s3AccessKey, s3SecretKey);
            IBackupAnalyzer backupAnalyzer = dbType.ToLower() switch
            {
                "mongodb" => new MongoBackupAnalyzer(),
                _ => throw new ArgumentException($"Unsupported database type: {dbType}")
            };

            AnsiConsole.MarkupLine($"[yellow]Fetching {dbType} backup list from S3...[/]");
            var s3Objects = await s3Service.ListObjectsAsync(s3Bucket, prefix);

            if (!s3Objects.S3Objects.Any())
            {
                AnsiConsole.MarkupLine("[red]No backups found.[/]");
                return;
            }

            // Sort backups by last modified date, most recent first
            var sortedObjects = s3Objects.S3Objects
                .OrderByDescending(x => x.LastModified)
                .ToList();

            if (latestOnly)
            {
                // If latest-only flag is set, only process the most recent backup
                sortedObjects = sortedObjects.Take(1).ToList();
            }

            AnsiConsole.MarkupLine($"[green]Found {(latestOnly ? "latest" : sortedObjects.Count.ToString())} backup{(latestOnly ? "" : "s")}[/]");

            await AnsiConsole.Progress()
                .AutoClear(false)
                .Columns(
                [
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn(),
                ])
                .StartAsync(async ctx =>
                {
                    var mainTask = ctx.AddTask($"[green]Processing Backup{(latestOnly ? "" : "s")}[/]", maxValue: sortedObjects.Count);
                    bool hasShownDetailedBackup = false;

                    var table = new Table()
                        .Border(TableBorder.Rounded)
                        .BorderColor(Color.Blue)
                        .Title($"[bold blue]{dbType} Backup Analysis Results{(latestOnly ? " (Latest Only)" : "")}[/]")
                        .Caption("[dim]Completed at " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "[/]");

                    table.AddColumn(new TableColumn("[bold]Backup File[/]").Width(25).NoWrap());
                    table.AddColumn(new TableColumn("[bold]Age[/]").Width(12).Centered());
                    table.AddColumn(new TableColumn("[bold]Status[/]").Width(3).Centered().NoWrap());
                    table.AddColumn(new TableColumn("[bold]Details[/]").Width(50));

                    foreach (var s3Object in sortedObjects)
                    {
                        var processingTask = ctx.AddTask($"[blue]Processing: {s3Object.Key}[/]", maxValue: 100);
                        await using var tempFile = new TempFile();

                        try
                        {
                            await s3Service.DownloadObjectAsync(s3Bucket, s3Object.Key, tempFile.Path);
                            processingTask.Increment(50);

                            var (timestamp, databases) = await backupAnalyzer.AnalyzeBackup(tempFile.Path);
                            processingTask.Increment(40);

                            var age = GetAgeDisplay(s3Object.LastModified);
                            var shortKey = TruncateFileName(s3Object.Key, 22);

                            if (databases.Any() && timestamp != null)
                            {
                                var dbCount = databases.Count();
                                if (!hasShownDetailedBackup)
                                {
                                    // Show detailed view for the first successful backup (most recent)
                                    var dbList = string.Join("\n", databases.OrderBy(x => x).Select(db => $"  • [dim cyan]{db}[/]"));
                                    table.AddRow(
                                        new Markup($"[green]{shortKey}[/]"),
                                        new Markup($"[blue]{age}[/]"),
                                        new Markup("[bold green]✓[/]"),
                                        new Markup($"[bold]Timestamp:[/] [blue]{timestamp}[/]\n[bold]Latest Backup - Databases ({dbCount}):[/]\n{dbList}")
                                    );
                                    hasShownDetailedBackup = true;
                                }
                                else
                                {
                                    // Show compact view for older backups
                                    table.AddRow(
                                        new Markup($"[green]{shortKey}[/]"),
                                        new Markup($"[blue]{age}[/]"),
                                        new Markup("[bold green]✓[/]"),
                                        new Markup($"[dim]Contains {dbCount} databases[/]")
                                    );
                                }
                            }
                            else
                            {
                                table.AddRow(
                                    new Markup($"[yellow]{shortKey}[/]"),
                                    new Markup($"[blue]{age}[/]"),
                                    new Markup("[bold yellow]⚠[/]"),
                                    new Markup("[yellow]No database information found[/]")
                                );
                            }

                            processingTask.Increment(10);
                            mainTask.Increment(1);
                        }
                        catch (Exception ex)
                        {
                            var age = GetAgeDisplay(s3Object.LastModified);
                            var shortKey = TruncateFileName(s3Object.Key, 22);
                            table.AddRow(
                                new Markup($"[red]{shortKey}[/]"),
                                new Markup($"[blue]{age}[/]"),
                                new Markup("[bold red]✗[/]"),
                                new Markup($"[red]Error:[/] [bold red]{ex.Message}[/]")
                            );
                            processingTask.Value = 100;
                            mainTask.Increment(1);
                        }

                        processingTask.Value = 100;
                    }

                    AnsiConsole.WriteLine();
                    AnsiConsole.Write(table);
                    AnsiConsole.WriteLine();
                });
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error in backup listing process: {ex.Message}[/]");
            Environment.Exit(1);
        }
    }

    private static string GetAgeDisplay(DateTime lastModified)
    {
        var timeSpan = DateTime.UtcNow - lastModified.ToUniversalTime();

        if (timeSpan.TotalMinutes < 1)
            return "just now";
        if (timeSpan.TotalHours < 1)
            return $"{timeSpan.Minutes}m ago";
        if (timeSpan.TotalDays < 1)
            return $"{timeSpan.Hours}h ago";
        if (timeSpan.TotalDays < 30)
            return $"{timeSpan.Days}d ago";
        if (timeSpan.TotalDays < 365)
            return $"{(int)(timeSpan.TotalDays / 30)}mo ago";
        return $"{(int)(timeSpan.TotalDays / 365)}y ago";
    }

    private static string TruncateFileName(string fileName, int maxLength)
    {
        if (fileName.Length <= maxLength) return fileName;

        var extension = Path.GetExtension(fileName);
        var nameWithoutExt = Path.GetFileNameWithoutExtension(fileName);
        var dirName = Path.GetDirectoryName(fileName);

        var availableLength = maxLength - 3; // for "..."
        if (!string.IsNullOrEmpty(extension))
            availableLength -= extension.Length;

        if (string.IsNullOrEmpty(dirName))
        {
            var truncatedName = nameWithoutExt.Substring(0, Math.Min(nameWithoutExt.Length, availableLength));
            return $"{truncatedName}...{extension}";
        }
        else
        {
            var truncatedDir = dirName.Length > (availableLength / 2)
                ? dirName.Substring(0, Math.Min(dirName.Length, availableLength / 2)) + "..."
                : dirName;
            var remainingLength = availableLength - truncatedDir.Length - 1; // -1 for path separator
            var truncatedName = nameWithoutExt.Substring(0, Math.Min(nameWithoutExt.Length, remainingLength));
            return $"{truncatedDir}/{truncatedName}...{extension}";
        }
    }
}

public class TempFile : IAsyncDisposable
{
    public string Path { get; }

    public TempFile()
    {
        Path = System.IO.Path.GetTempFileName();
    }

    public async ValueTask DisposeAsync()
    {
        if (File.Exists(Path))
        {
            try
            {
                File.Delete(Path);
            }
            catch
            {
                // Best effort cleanup
            }
        }
    }
}
