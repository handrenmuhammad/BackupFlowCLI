using System.CommandLine;
using System.CommandLine.IO;
using System.Diagnostics;
using System.Text;
using System.Runtime.InteropServices;
using Amazon.S3;
using Amazon.S3.Model;
using BackupFlowCLI;
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
        var restoreCommand = new Command("restore", @"Restores a backup of the specified database from S3.
        Examples:
          dbbackup restore --type=mongodb --s3-endpoint=s3.amazonaws.com --s3-access-key=YOUR_ACCESS_KEY --s3-secret-key=YOUR_SECRET_KEY --s3-bucket=my-backup-bucket --prefix=prod/
          dbbackup restore --type=mongodb --connection-string=""mongodb://localhost:27017"" --s3-endpoint=s3.amazonaws.com --s3-access-key=YOUR_ACCESS_KEY --s3-secret-key=YOUR_SECRET_KEY --s3-bucket=my-backup-bucket
          dbbackup restore --type=postgresql --connection-string=""postgresql://localhost:5432"" --s3-endpoint=s3.amazonaws.com --s3-access-key=YOUR_ACCESS_KEY --s3-secret-key=YOUR_SECRET_KEY --s3-bucket=my-backup-bucket
        ");
        var listCommand = new Command("list", @"Lists all database backups from S3.
        Examples:
          dbbackup list --type=mongodb --s3-endpoint=s3.amazonaws.com --s3-access-key=YOUR_ACCESS_KEY --s3-secret-key=YOUR_SECRET_KEY --s3-bucket=my-backup-bucket
          dbbackup list --type=postgresql --s3-endpoint=s3.amazonaws.com --s3-access-key=YOUR_ACCESS_KEY --s3-secret-key=YOUR_SECRET_KEY --s3-bucket=my-backup-bucket --prefix=prod/
          dbbackup list --type=mongodb --latest-only --s3-endpoint=s3.amazonaws.com --s3-access-key=YOUR_ACCESS_KEY --s3-secret-key=YOUR_SECRET_KEY --s3-bucket=my-backup-bucket
        ");

        // Database options
        var typeOption = new Option<string>(
            aliases: new[] { "-t", "--type" },
            description: "Database type (mongodb or postgresql)")
        { IsRequired = true };

        var connectionStringOption = new Option<string>(
            aliases: new[] { "-c", "--connection-string" },
            description: "MongoDB connection string");

        var hostOption = new Option<string>(
            aliases: new[] { "-h", "--host" },
            description: "Database host");

        var portOption = new Option<int>(
            aliases: new[] { "--port" },
            description: "Database port");

        var usernameOption = new Option<string>(
            aliases: new[] { "-u", "--username" },
            description: "Database username");

        var passwordOption = new Option<string>(
            aliases: new[] { "--password" },
            description: "Database password");

        var databasesOption = new Option<string[]>("--databases", description: "Specific databases to backup (if not specified, all databases will be backed up)") { AllowMultipleArgumentsPerToken = true };
        databasesOption.AddAlias("-d");

        var incrementalOption = new Option<bool>(
            aliases: new[] { "-i", "--incremental" },
            description: "Include incremental changes for point-in-time recovery capabilities");

        var intervalOption = new Option<int>("--interval", description: "Interval in minutes for continuous oplog backup") { IsRequired = false };
        intervalOption.SetDefaultValue(10);

        // S3 options
        var s3EndpointOption = new Option<string>(
            aliases: new[] { "-e", "--s3-endpoint" },
            description: "S3 endpoint URL")
        { IsRequired = true };

        var s3AccessKeyOption = new Option<string>(
            aliases: new[] { "-k", "--s3-access-key" },
            description: "S3 access key")
        { IsRequired = true };

        var s3SecretKeyOption = new Option<string>(
            aliases: new[] { "-s", "--s3-secret-key" },
            description: "S3 secret key")
        { IsRequired = true };

        var s3BucketOption = new Option<string>(
            aliases: new[] { "-b", "--s3-bucket" },
            description: "S3 bucket name")
        { IsRequired = true };

        var prefixOption = new Option<string>(
            aliases: new[] { "--prefix" },
            description: "Backup prefix/path in bucket")
        { IsRequired = true };

        var pgBackupTypeOption = new Option<string>(
            "--pg-backup-type",
            description: "PostgreSQL backup type (basebackup or dump)"
        );
        pgBackupTypeOption.SetDefaultValue("dump");
        pgBackupTypeOption.AddAlias("-t");
        var latestOnlyOption = new Option<bool>("--latest-only", description: "Show only the latest backup's database names", getDefaultValue: () => false);
        latestOnlyOption.AddAlias("-l");
        // backup command options
        backupCommand.AddOption(typeOption);
        backupCommand.AddOption(hostOption);
        backupCommand.AddOption(portOption);
        backupCommand.AddOption(usernameOption);
        backupCommand.AddOption(passwordOption);
        backupCommand.AddOption(databasesOption);
        backupCommand.AddOption(incrementalOption);
        backupCommand.AddOption(s3EndpointOption);
        backupCommand.AddOption(s3AccessKeyOption);
        backupCommand.AddOption(s3SecretKeyOption);
        backupCommand.AddOption(s3BucketOption);
        backupCommand.AddOption(prefixOption);
        backupCommand.AddOption(connectionStringOption);
        backupCommand.AddOption(pgBackupTypeOption);
        backupCommand.AddOption(intervalOption);
        // list command options
        listCommand.AddOption(typeOption);
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


            var backupOptions = new BackupOptions()
            {
                DbType = context.ParseResult.GetValueForOption<string>(typeOption)!,
                Host = context.ParseResult.GetValueForOption<string>(hostOption)!,
                Port = context.ParseResult.GetValueForOption<int>(portOption),
                Username = context.ParseResult.GetValueForOption<string>(usernameOption)!,
                Password = context.ParseResult.GetValueForOption<string>(passwordOption)!,
                Databases = context.ParseResult.GetValueForOption<string[]>(databasesOption),
                Incremental = context.ParseResult.GetValueForOption<bool>(incrementalOption),
                S3Endpoint = context.ParseResult.GetValueForOption<string>(s3EndpointOption)!,
                S3AccessKey = context.ParseResult.GetValueForOption<string>(s3AccessKeyOption)!,
                S3SecretKey = context.ParseResult.GetValueForOption<string>(s3SecretKeyOption)!,
                S3Bucket = context.ParseResult.GetValueForOption<string>(s3BucketOption)!,
                Prefix = context.ParseResult.GetValueForOption<string>(prefixOption)!,
                ConnectionString = context.ParseResult.GetValueForOption<string>(connectionStringOption)!,
                PgBackupType = context.ParseResult.GetValueForOption<string>(pgBackupTypeOption)!,
                Interval = context.ParseResult.GetValueForOption<int>(intervalOption)
            };

            if (backupOptions.DbType == "postgresql" && backupOptions.PgBackupType == "dump" && backupOptions.Incremental)
            {
                AnsiConsole.MarkupLine("[red]Error: incremental restore is not supported for PostgreSQL dump backup type[/]");
                Environment.Exit(1);
            }

            if (!string.IsNullOrEmpty(backupOptions.ConnectionString))
            {
                if (!string.IsNullOrEmpty(backupOptions.Host) || backupOptions.Port != 0 || !string.IsNullOrEmpty(backupOptions.Username) || !string.IsNullOrEmpty(backupOptions.Password))
                {
                    AnsiConsole.MarkupLine("[red]Error: when using connection-string, host, port, username and password should not be provided[/]");
                    Environment.Exit(1);
                }
            }
            if (!backupOptions.UseConnectionString)
            {
                if (string.IsNullOrEmpty(backupOptions.Host) || backupOptions.Port == 0 || string.IsNullOrEmpty(backupOptions.Username) || string.IsNullOrEmpty(backupOptions.Password))
                {
                    AnsiConsole.MarkupLine("[red]Error: host, port, username and password are required when using connection-string is not provided[/]");
                    Environment.Exit(1);
                }
            }

            await CreateBackup(backupOptions);
        });

        listCommand.SetHandler(async (context) =>
        {
            var listOptions = new ListOptions()
            {
                DbType = context.ParseResult.GetValueForOption<string>(typeOption)!,
                LatestOnly = context.ParseResult.GetValueForOption<bool>(latestOnlyOption),
                S3Endpoint = context.ParseResult.GetValueForOption<string>(s3EndpointOption)!,
                S3AccessKey = context.ParseResult.GetValueForOption<string>(s3AccessKeyOption)!,
                S3SecretKey = context.ParseResult.GetValueForOption<string>(s3SecretKeyOption)!,
                S3Bucket = context.ParseResult.GetValueForOption<string>(s3BucketOption)!,
                Prefix = context.ParseResult.GetValueForOption<string>(prefixOption)!
            };
            await ListBackups(listOptions);
        });

        // restore command options
        restoreCommand.AddOption(typeOption);
        restoreCommand.AddOption(hostOption);
        restoreCommand.AddOption(portOption);
        restoreCommand.AddOption(usernameOption);
        restoreCommand.AddOption(passwordOption);
        restoreCommand.AddOption(databasesOption);
        restoreCommand.AddOption(incrementalOption);
        restoreCommand.AddOption(s3EndpointOption);
        restoreCommand.AddOption(s3AccessKeyOption);
        restoreCommand.AddOption(s3SecretKeyOption);
        restoreCommand.AddOption(s3BucketOption);
        restoreCommand.AddOption(prefixOption);
        restoreCommand.AddOption(connectionStringOption);
        restoreCommand.SetHandler(async (context) =>
        {
            var restoreOptions = new BackupOptions()
            {
                DbType = context.ParseResult.GetValueForOption<string>(typeOption)!,
                ConnectionString = context.ParseResult.GetValueForOption<string>(connectionStringOption)!,
                Host = context.ParseResult.GetValueForOption<string>(hostOption)!,
                Port = context.ParseResult.GetValueForOption<int>(portOption),
                Username = context.ParseResult.GetValueForOption<string>(usernameOption)!,
                Password = context.ParseResult.GetValueForOption<string>(passwordOption)!,
                S3Endpoint = context.ParseResult.GetValueForOption<string>(s3EndpointOption)!,
                S3AccessKey = context.ParseResult.GetValueForOption<string>(s3AccessKeyOption)!,
                S3SecretKey = context.ParseResult.GetValueForOption<string>(s3SecretKeyOption)!,
                S3Bucket = context.ParseResult.GetValueForOption<string>(s3BucketOption)!,
                Prefix = context.ParseResult.GetValueForOption<string>(prefixOption)!,
                Databases = context.ParseResult.GetValueForOption<string[]>(databasesOption),
                Incremental = context.ParseResult.GetValueForOption<bool>(incrementalOption),
            };

            if (!string.IsNullOrEmpty(restoreOptions.ConnectionString))
            {
                if (!string.IsNullOrEmpty(restoreOptions.Host) || restoreOptions.Port != 0 || !string.IsNullOrEmpty(restoreOptions.Username) || !string.IsNullOrEmpty(restoreOptions.Password))
                {
                    AnsiConsole.MarkupLine("[red]Error: only one of connection-string or host, port, username and password should be provided[/]");
                    Environment.Exit(1);
                }
            }
            if (restoreOptions.DbType == "postgresql" && restoreOptions.Incremental)
            {
                AnsiConsole.MarkupLine("[red]Error: Currently incremental restore is not supported for PostgreSQL[/]");
                Environment.Exit(1);
            }
            if (!restoreOptions.UseConnectionString)
            {
                if (string.IsNullOrEmpty(restoreOptions.Host) || restoreOptions.Port == 0 || string.IsNullOrEmpty(restoreOptions.Username) || string.IsNullOrEmpty(restoreOptions.Password))
                {
                    AnsiConsole.MarkupLine("[red]Error: host, port, username and password are required when using connection-string is not provided[/]");
                    Environment.Exit(1);
                }
            }

            await RestoreBackup(restoreOptions);
        });

        backupCommand.Description = $$"""
[1;36mBackup a database to S3-compatible storage.[0m

[1;33mUSAGE:[0m
  dbbackup backup --type <mongodb|postgresql> [options]

[1;33mOPTIONS:[0m
  [1;32m-t, --type[0m                     [1;31mRequired.[0m Database type (mongodb or postgresql)
  [1;32m-c, --connection-string[0m        MongoDB connection string (for MongoDB only)
  [1;32m-h, --host[0m                     Database host (for PostgreSQL only)
  [1;32m--port[0m                         Database port (for PostgreSQL only)
  [1;32m-u, --username[0m                 Database username (for PostgreSQL only)
  [1;32m--password[0m                     Database password (for PostgreSQL only)
  [1;32m-e, --s3-endpoint[0m             [1;31mRequired.[0m S3 endpoint URL
  [1;32m-k, --s3-access-key[0m           [1;31mRequired.[0m S3 access key
  [1;32m-s, --s3-secret-key[0m           [1;31mRequired.[0m S3 secret key
  [1;32m-b, --s3-bucket[0m               [1;31mRequired.[0m S3 bucket name
  [1;32m--prefix[0m                      [1;31mRequired.[0m Backup prefix/path in bucket
  [1;32m-i, --incremental[0m             Include incremental changes for point-in-time recovery capabilities

[1;33mEXAMPLES:[0m
  [1;35mMongoDB:[0m
    dbbackup backup -t mongodb -c mongodb://localhost:27017 -e localhost:9000 -k guest -s password -b dev --prefix mongo/backups -i

  [1;35mPostgreSQL:[0m
    dbbackup backup -t postgresql -h localhost --port 5432 -u guest --password guest -e localhost:9000 -k guest -s password -b dev --prefix postgres/backups -i
""";

        restoreCommand.Description = $$"""
[1;36mRestore a database from S3-compatible storage.[0m

[1;33mUSAGE:[0m
  dbbackup restore --type <mongodb|postgresql> [options]

[1;33mOPTIONS:[0m
  [1;32m-t, --type[0m                     [1;31mRequired.[0m Database type (mongodb or postgresql)
  [1;32m-c, --connection-string[0m        MongoDB connection string (for MongoDB only)
  [1;32m-h, --host[0m                     Database host (for PostgreSQL only)
  [1;32m--port[0m                         Database port (for PostgreSQL only)
  [1;32m-u, --username[0m                 Database username (for PostgreSQL only)
  [1;32m--password[0m                     Database password (for PostgreSQL only)
  [1;32m--database[0m                     Database name to restore (for PostgreSQL only)
  [1;32m-e, --s3-endpoint[0m             [1;31mRequired.[0m S3 endpoint URL
  [1;32m-k, --s3-access-key[0m           [1;31mRequired.[0m S3 access key
  [1;32m-s, --s3-secret-key[0m           [1;31mRequired.[0m S3 secret key
  [1;32m-b, --s3-bucket[0m               [1;31mRequired.[0m S3 bucket name
  [1;32m--prefix[0m                      [1;31mRequired.[0m Backup prefix/path in bucket
  [1;32m-i, --incremental[0m             Include incremental changes in the restore process

[1;33mEXAMPLES:[0m
  [1;35mMongoDB:[0m
    dbbackup restore -t mongodb -c mongodb://localhost:27017 -e localhost:9000 -k guest -s password -b dev --prefix mongo/backups -i

  [1;35mPostgreSQL:[0m
    dbbackup restore -t postgresql -h localhost --port 5432 -u guest --password guest --database mydb -e localhost:9000 -k guest -s password -b dev --prefix postgres/backups -i
""";

        return await rootCommand.InvokeAsync(args);
    }

    public static async Task CreateBackup(BackupOptions backupOptions)
    {
        try
        {
            AnsiConsole.MarkupLine($"[yellow]Creating {backupOptions.DbType} backup...[/]");

            string backupPath;
            DateTime timestamp;

            switch (backupOptions.DbType.ToLower())
            {
                case "mongodb":
                    MongoOplogBackupService? oplogBackupService = null;
                    try
                    {
                        if (backupOptions.UseConnectionString)
                        {
                            var mongoBackupService = new MongoBackupService(backupOptions.ConnectionString!);
                            (backupPath, timestamp) = await mongoBackupService.CreateBackup(backupOptions.Databases ?? Array.Empty<string>(), backupOptions.Incremental);

                            if (backupOptions.Incremental)
                            {
                                if (await mongoBackupService.IsReplicaSetMember())
                                {
                                    oplogBackupService = new MongoOplogBackupService(
                                        backupOptions.ConnectionString!,
                                        backupOptions.S3Endpoint,
                                        backupOptions.S3AccessKey,
                                        backupOptions.S3SecretKey,
                                        backupOptions.S3Bucket,
                                        backupOptions.Prefix,
                                        intervalMinutes: backupOptions.Interval,
                                        useConnectionString: backupOptions.UseConnectionString
                                    );
                                    AnsiConsole.MarkupLine("[blue]Including oplog in backup for point-in-time recovery[/]");
                                }
                                else
                                {
                                    AnsiConsole.MarkupLine("[yellow]Warning: Oplog backup requested but the target MongoDB instance is not a replica set member. Continuing without oplog backup.[/]");
                                }
                            }
                        }
                        else
                        {
                            var mongoBackupService = new MongoBackupService(backupOptions.Host!, backupOptions.Port, backupOptions.Username!, backupOptions.Password!);
                            (backupPath, timestamp) = await mongoBackupService.CreateBackup(backupOptions.Databases ?? Array.Empty<string>(), backupOptions.Incremental);

                            if (backupOptions.Incremental)
                            {
                                if (await mongoBackupService.IsReplicaSetMember())
                                {
                                    oplogBackupService = new MongoOplogBackupService(
                                        backupOptions.Host!,
                                        backupOptions.Port,
                                        backupOptions.Username!,
                                        backupOptions.Password!,
                                        backupOptions.S3Endpoint,
                                        backupOptions.S3AccessKey,
                                        backupOptions.S3SecretKey,
                                        backupOptions.S3Bucket,
                                        backupOptions.Prefix,
                                        intervalMinutes: backupOptions.Interval,
                                        useConnectionString: backupOptions.UseConnectionString
                                    );
                                    AnsiConsole.MarkupLine("[blue]Including oplog in backup for point-in-time recovery[/]");
                                }
                                else
                                {
                                    AnsiConsole.MarkupLine("[yellow]Warning: Oplog backup requested but the target MongoDB instance is not a replica set member. Continuing without oplog backup.[/]");
                                }
                            }
                        }

                        AnsiConsole.MarkupLine($"[green]Backup created successfully at: {backupPath}[/]");


                        AnsiConsole.MarkupLine("[yellow]Uploading backup to S3...[/]");
                        var s3Service = new S3Service(backupOptions.S3Endpoint, backupOptions.S3AccessKey, backupOptions.S3SecretKey);

                        // check if bucket exists
                        await s3Service.EnsureBucketExistsAsync(backupOptions.S3Bucket);

                        var s3Key = $"{backupOptions.Prefix?.TrimEnd('/')}/{Path.GetFileName(backupPath)}";

                        await using (var fileStream = File.OpenRead(backupPath))
                        {
                            var putObjectRequest = new PutObjectRequest
                            {
                                BucketName = backupOptions.S3Bucket,
                                Key = s3Key,
                                InputStream = fileStream
                            };

                            await s3Service._s3Client.PutObjectAsync(putObjectRequest);
                        }

                        AnsiConsole.MarkupLine($"[green]Backup uploaded successfully to S3: {s3Key}[/]");

                        // Start continuous oplog backup if incremental backup is requested
                        if (oplogBackupService != null)
                        {
                            await oplogBackupService.StartContinuousBackup();
                            AnsiConsole.MarkupLine("[green]Press Ctrl+C to stop the continuous oplog backup[/]");

                            // Handle graceful shutdown
                            Console.CancelKeyPress += (sender, e) =>
                            {
                                e.Cancel = true; // Prevent the process from terminating immediately
                                AnsiConsole.MarkupLine("[yellow]Stopping continuous oplog backup...[/]");
                                oplogBackupService.Dispose();
                                AnsiConsole.MarkupLine("[green]Continuous oplog backup stopped[/]");
                            };

                            // Keep the application running
                            var tcs = new TaskCompletionSource();
                            Console.CancelKeyPress += (sender, e) => tcs.SetResult();
                            await tcs.Task;
                        }

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
                    finally
                    {
                        oplogBackupService?.Dispose();
                    }
                    break;

                case "postgresql":
                    try
                    {
                        PostgresBackupService postgresBackupService;
                        PostgresWalBackupService? walBackupService = null;

                        if (backupOptions.UseConnectionString)
                        {
                            postgresBackupService = new PostgresBackupService(backupOptions.ConnectionString!);
                            if (backupOptions.Incremental)
                            {
                                walBackupService = new PostgresWalBackupService(
                                    backupOptions.ConnectionString!,
                                    backupOptions.S3Endpoint,
                                    backupOptions.S3AccessKey,
                                    backupOptions.S3SecretKey,
                                    backupOptions.S3Bucket,
                                    backupOptions.Prefix,
                                    intervalMinutes: backupOptions.Interval,
                                    useConnectionString: backupOptions.UseConnectionString
                                );
                            }
                        }
                        else
                        {
                            postgresBackupService = new PostgresBackupService(backupOptions.Host!, backupOptions.Port, backupOptions.Username!, backupOptions.Password!);
                            if (backupOptions.Incremental)
                            {
                                walBackupService = new PostgresWalBackupService(
                                    backupOptions.Host!,
                                    backupOptions.Port,
                                    backupOptions.Username!,
                                    backupOptions.Password!,
                                    backupOptions.S3Endpoint,
                                    backupOptions.S3AccessKey,
                                    backupOptions.S3SecretKey,
                                    backupOptions.S3Bucket,
                                    backupOptions.Prefix,
                                    intervalMinutes: backupOptions.Interval,
                                    useConnectionString: backupOptions.UseConnectionString
                                );
                            }
                        }

                        var backupType = backupOptions.PgBackupType!.ToLower() == "dump" ? PostgresBackupType.Dump : PostgresBackupType.BaseBackup;

                        (backupPath, timestamp) = await postgresBackupService.CreateBackup(
                            backupType: backupType,
                            includeWal: backupOptions.Incremental,
                            databases: backupOptions.Databases
                        );

                        AnsiConsole.MarkupLine($"[green]Backup created successfully at: {backupPath}[/]");

                        AnsiConsole.MarkupLine("[yellow]Uploading backup to S3...[/]");
                        var s3Service = new S3Service(backupOptions.S3Endpoint, backupOptions.S3AccessKey, backupOptions.S3SecretKey);

                        // check if bucket exists
                        await s3Service.EnsureBucketExistsAsync(backupOptions.S3Bucket);

                        var s3Key = $"{backupOptions.Prefix?.TrimEnd('/')}/{Path.GetFileName(backupPath)}";

                        await using (var fileStream = File.OpenRead(backupPath))
                        {
                            var putObjectRequest = new PutObjectRequest
                            {
                                BucketName = backupOptions.S3Bucket,
                                Key = s3Key,
                                InputStream = fileStream
                            };

                            await s3Service._s3Client.PutObjectAsync(putObjectRequest);
                        }

                        AnsiConsole.MarkupLine($"[green]Backup uploaded successfully to S3: {s3Key}[/]");

                        // start continuous WAL archiving if requested and if using basebackup
                        if (walBackupService != null && backupType == PostgresBackupType.BaseBackup)
                        {
                            await walBackupService.StartContinuousBackup();
                            AnsiConsole.MarkupLine("[green]Press Ctrl+C to stop the continuous WAL archiving[/]");

                            // Handle graceful shutdown
                            Console.CancelKeyPress += (sender, e) =>
                            {
                                e.Cancel = true; // Prevent the process from terminating immediately
                                AnsiConsole.MarkupLine("[yellow]Stopping continuous WAL archiving...[/]");
                                walBackupService.Dispose();
                                AnsiConsole.MarkupLine("[green]Continuous WAL archiving stopped[/]");
                            };

                            // Keep the application running
                            var tcs = new TaskCompletionSource();
                            Console.CancelKeyPress += (sender, e) => tcs.SetResult();
                            await tcs.Task;
                        }

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
                        AnsiConsole.MarkupLine($"[red]Error creating PostgreSQL backup: {ex.Message}[/]");
                        Environment.Exit(1);
                    }
                    break;

                default:
                    throw new ArgumentException($"Unsupported database type: {backupOptions.DbType}");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error creating backup: {ex.Message}[/]");
            Environment.Exit(1);
        }
    }

    public static async Task ListBackups(ListOptions listOptions)
    {
        try
        {
            var s3Service = new S3Service(listOptions.S3Endpoint, listOptions.S3AccessKey, listOptions.S3SecretKey);
            IBackupAnalyzer backupAnalyzer = listOptions.DbType.ToLower() switch
            {
                "mongodb" => new MongoBackupAnalyzer(),
                "postgresql" => new PostgresBackupAnalyzer(),
                _ => throw new ArgumentException($"Unsupported database type: {listOptions.DbType}")
            };

            AnsiConsole.MarkupLine($"[yellow]Fetching {listOptions.DbType} backup list from S3...[/]");
            AnsiConsole.MarkupLine($"[yellow]Prefix: {listOptions.Prefix}[/]");
            AnsiConsole.MarkupLine($"[yellow]Bucket: {listOptions.S3Bucket}[/]");
            AnsiConsole.MarkupLine($"[yellow]Endpoint: {listOptions.S3Endpoint}[/]");

            var s3Objects = await s3Service.ListObjectsAsync(listOptions.S3Bucket, listOptions.Prefix);

            if (s3Objects.S3Objects.Count == 0)
            {
                AnsiConsole.MarkupLine("[red]No backups found.[/]");
                return;
            }

            // filter objects based on database type
            var filteredObjects = s3Objects.S3Objects
                .Where(x =>
                {
                    var fileName = Path.GetFileName(x.Key).ToLower();
                    return listOptions.DbType.ToLower() switch
                    {
                        "postgresql" => fileName.StartsWith("postgres_"),
                        "mongodb" => !fileName.StartsWith("postgres_"),
                        _ => true
                    };
                })
                .ToList();

            if (filteredObjects.Count == 0)
            {
                AnsiConsole.MarkupLine($"[red]No {listOptions.DbType} backups found.[/]");
                return;
            }

            var sortedObjects = filteredObjects
                .OrderByDescending(x => x.LastModified)
                .ToList();

            if (listOptions.LatestOnly)
            {
                // if latestOnly is true, then only the latest backup is shown
                sortedObjects = sortedObjects.Take(1).ToList();
            }

            AnsiConsole.MarkupLine($"[green]Found {(listOptions.LatestOnly ? "latest" : sortedObjects.Count.ToString())} backup{(listOptions.LatestOnly ? "" : "s")}[/]");

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
                    var mainTask = ctx.AddTask($"[green]Processing Backup{(listOptions.LatestOnly ? "" : "s")}[/]", maxValue: sortedObjects.Count);
                    bool hasShownDetailedBackup = false;

                    var table = new Table()
                        .Border(TableBorder.Rounded)
                        .BorderColor(Color.Blue)
                        .Title($"[bold blue]{listOptions.DbType} Backup Analysis Results{(listOptions.LatestOnly ? " (Latest Only)" : "")}[/]")
                        .Caption("[dim]Completed at " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "[/]");

                    table.AddColumn(new TableColumn("[bold]Backup File[/]").Width(25).NoWrap());
                    table.AddColumn(new TableColumn("[bold]Age[/]").Width(12).Centered());
                    table.AddColumn(new TableColumn("[bold]Status[/]").Width(3).Centered().NoWrap());
                    table.AddColumn(new TableColumn("[bold]Details[/]").Width(50));

                    foreach (var s3Object in sortedObjects)
                    {
                        var processingTask = ctx.AddTask($"[blue]Processing: {s3Object.Key}[/]", maxValue: 100);

                        //store the backup file in a temporary file
                        await using var tempFile = new TempFile();

                        try
                        {
                            await s3Service.DownloadObjectAsync(listOptions.S3Bucket, s3Object.Key, tempFile.Path);
                            processingTask.Increment(50);

                            var (timestamp, databases) = await backupAnalyzer.AnalyzeBackup(tempFile.Path);
                            processingTask.Increment(40);

                            var age = ConvertToAge(s3Object.LastModified);
                            var shortKey = TruncateFileName(s3Object.Key, 22);

                            if (databases.Count > 0 && timestamp != null)
                            {
                                var dbCount = databases.Count;
                                if (!hasShownDetailedBackup)
                                {
                                    // show detailed view for the first successful backup most recent
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
                            var age = ConvertToAge(s3Object.LastModified);
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

    private static string ConvertToAge(DateTime lastModified)
    {
        var timeSpan = DateTime.UtcNow - lastModified.ToUniversalTime();

        if (timeSpan.TotalMinutes < 1)
            return $"{timeSpan.Seconds}s ago";
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
                ? dirName[..Math.Min(dirName.Length, availableLength / 2)] + "..."
                : dirName;
            var remainingLength = availableLength - truncatedDir.Length - 1; // -1 for path separator
            var truncatedName = nameWithoutExt.Substring(0, Math.Min(nameWithoutExt.Length, remainingLength));
            return $"{truncatedDir}/{truncatedName}...{extension}";
        }
    }

    public static async Task RestoreBackup(BackupOptions restoreOptions)
    {
        try
        {
            var s3Service = new S3Service(restoreOptions.S3Endpoint, restoreOptions.S3AccessKey, restoreOptions.S3SecretKey);

            AnsiConsole.MarkupLine($"[yellow]Fetching available {restoreOptions.DbType} backups from S3...[/]");
            var s3Objects = await s3Service.ListObjectsAsync(restoreOptions.S3Bucket, restoreOptions.Prefix);

            if (s3Objects.S3Objects.Count == 0)
            {
                AnsiConsole.MarkupLine("[red]No backups found.[/]");
                return;
            }

            // Sort backups by last modified date
            var sortedObjects = s3Objects.S3Objects
                .Where(x => !x.Key.Contains("/oplogs/")) // Filter out oplog backups from main selection
                .OrderByDescending(x => x.LastModified)
                .ToList();

            // Create a selection prompt for the user to choose a backup
            var selection = AnsiConsole.Prompt(
                new SelectionPrompt<Amazon.S3.Model.S3Object>()
                    .Title("Select a backup to restore:")
                    .PageSize(10)
                    .UseConverter(obj => $"{obj.Key} ({ConvertToAge(obj.LastModified)})")
                    .AddChoices(sortedObjects)
            );

            AnsiConsole.MarkupLine($"[yellow]Downloading backup {selection.Key}...[/]");

            await using var tempFile = new TempFile();
            await s3Service.DownloadObjectAsync(restoreOptions.S3Bucket, selection.Key, tempFile.Path);

            AnsiConsole.MarkupLine($"[yellow]Restoring {restoreOptions.DbType} backup...[/]");

            switch (restoreOptions.DbType.ToLower())
            {
                case "mongodb":
                    MongoRestoreService mongoRestoreService;
                    if (restoreOptions.UseConnectionString)
                    {
                        mongoRestoreService = new MongoRestoreService(restoreOptions.ConnectionString!);
                    }
                    else
                    {
                        mongoRestoreService = new MongoRestoreService(restoreOptions.Host!, restoreOptions.Port, restoreOptions.Username!, restoreOptions.Password!);
                    }

                    // first restring the main backup
                    await mongoRestoreService.RestoreBackup(tempFile.Path, restoreOptions.Databases, false);

                    // if oplog arg was provided, restore the oplogs as well
                    if (restoreOptions.Incremental)
                    {
                        // list available oplog backups
                        var oplogPrefix = $"{restoreOptions.Prefix?.TrimEnd('/')}/oplogs/";
                        var oplogObjects = await s3Service.ListObjectsAsync(restoreOptions.S3Bucket, oplogPrefix);
                        var sortedOplogObjects = oplogObjects.S3Objects
                            .Where(x => x.LastModified > selection.LastModified) // only get oplogs after the backup time
                            .OrderByDescending(x => x.LastModified)
                            .ToList();

                        if (sortedOplogObjects.Count > 0)
                        {
                            // choosing the point in time to restore to
                            var latestOplog = sortedOplogObjects.Last();
                            var restoreToTime = AnsiConsole.Prompt(
                                new SelectionPrompt<Amazon.S3.Model.S3Object>()
                                    .Title("Select point in time to restore to (oplog backup timestamp):")
                                    .PageSize(10)
                                    .UseConverter(obj => $"{obj.Key} ({ConvertToAge(obj.LastModified)})")
                                    .AddChoices(sortedOplogObjects)
                            );

                            AnsiConsole.MarkupLine("[yellow]Restoring oplogs for point-in-time recovery...[/]");

                            // restoring each oplog up to the selected point in time
                            foreach (var oplog in sortedOplogObjects.TakeWhile(x => x.LastModified <= restoreToTime.LastModified))
                            {
                                await using var oplogTempFile = new TempFile();
                                AnsiConsole.MarkupLine($"[grey]Applying oplog: {oplog.Key}[/]");
                                await s3Service.DownloadObjectAsync(restoreOptions.S3Bucket, oplog.Key, oplogTempFile.Path);
                                await mongoRestoreService.RestoreOplog(oplogTempFile.Path);
                            }

                            AnsiConsole.MarkupLine("[green]Oplog restore completed[/]");
                        }
                        else
                        {
                            AnsiConsole.MarkupLine("[yellow]No oplog backups found after this backup timestamp[/]");
                        }
                    }
                    break;

                case "postgresql":
                    var postgresRestoreService = restoreOptions.UseConnectionString
                        ? new PostgresRestoreService(restoreOptions.ConnectionString!)
                        : new PostgresRestoreService(restoreOptions.Host!, restoreOptions.Port, restoreOptions.Username!, restoreOptions.Password!);

                    await postgresRestoreService.RestoreBackup(tempFile.Path, restoreOptions.Databases);
                    break;

                default:
                    throw new ArgumentException($"Unsupported database type: {restoreOptions.DbType}");
            }

            AnsiConsole.MarkupLine($"[green]Successfully restored backup: {selection.Key}[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error restoring backup: {ex.Message}[/]");
            Environment.Exit(1);
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
