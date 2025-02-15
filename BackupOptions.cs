using System.CommandLine;

namespace DbBackupCLI;

public class BackupOptions
{
    public required string DbType { get; set; }
    public string? Host { get; set; }
    public int Port { get; set; } = 0;
    public string? Username { get; set; }
    public string? Password { get; set; }
    public string[]? Databases { get; set; }
    public required bool Incremental { get; set; }
    public required string S3Endpoint { get; set; }
    public required string S3AccessKey { get; set; }
    public required string S3SecretKey { get; set; }
    public required string S3Bucket { get; set; }
    public required string Prefix { get; set; }
    public string? ConnectionString { get; set; }
    public string? PgBackupType { get; set; }

    public int Interval { get; set; } = 10;
    public bool UseConnectionString => !string.IsNullOrEmpty(ConnectionString);

}

public class ListOptions
{
    public required string DbType { get; set; }
    public bool LatestOnly { get; set; }
    public required string S3Endpoint { get; set; }
    public required string S3AccessKey { get; set; }
    public required string S3SecretKey { get; set; }
    public required string S3Bucket { get; set; }
    public required string Prefix { get; set; }
}
