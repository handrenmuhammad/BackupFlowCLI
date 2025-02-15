# BackupFlowCLI

A powerful CLI tool for seamless database backup and restore operations, supporting MongoDB and PostgreSQL databases with S3-compatible storage integration. Features comprehensive point-in-time recovery capabilities for both database systems.

## Features

- **Multi-Database Support**
  - MongoDB backup and restore
  - PostgreSQL backup and restore
  - Support for both full database and selective database backups

- **Advanced Backup Types**
  - MongoDB:
    - Full database dumps
    - Incremental backups with oplog support
    - Point-in-time recovery through continuous oplog archiving
    - Automated oplog tailing and archiving
  - PostgreSQL:
    - pg_dump backups
    - pg_basebackup support
    - Continuous WAL (Write-Ahead Log) archiving
    - Point-in-time recovery through WAL segments

- **Point-in-Time Recovery (PITR)**
  - MongoDB:
    - Continuous oplog capturing
    - Restore to any point in time using oplog replay
    - Configurable oplog retention period
  - PostgreSQL:
    - Continuous WAL archiving
    - Restore to any transaction point
    - Timeline-based recovery options

- **S3 Integration**
  - Compatible with any S3-compatible storage
  - Configurable bucket and prefix management
  - Secure credential handling
  - Efficient storage of incremental backups

- **Flexible Connection Options**
  - Support for connection strings
  - Individual credential parameters
  - Custom host and port configuration

## Prerequisites

- .NET 9.0 or higher
- MongoDB Tools (for MongoDB operations)
- PostgreSQL Client Tools (for PostgreSQL operations)
- Access to an S3-compatible storage service

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/BackupFlowCLI.git
```

2. Build the project:
```bash
cd BackupFlowCLI
dotnet build
```

3. Run the application:
```bash
dotnet run
```

## Usage

### Basic Commands

1. **Create a Backup**
```bash
backupflowcli backup --type <mongodb|postgresql> [options]
```

2. **Restore a Backup**
```bash
backupflowcli restore --type <mongodb|postgresql> [options]
```

3. **List Available Backups**
```bash
backupflowcli list --type <mongodb|postgresql> [options]
```

### Example Commands

#### MongoDB Backup with PITR
```bash
backupflowcli backup \
  --type mongodb \
  --connection-string "mongodb://localhost:27017" \
  --s3-endpoint localhost:9000 \
  --s3-access-key your_access_key \
  --s3-secret-key your_secret_key \
  --s3-bucket your_bucket \
  --prefix mongo/backups \
  --incremental \
  --interval 10
```

#### PostgreSQL Backup with PITR
```bash
backupflowcli backup \
  --type postgresql \
  --host localhost \
  --port 5432 \
  --username your_username \
  --password your_password \
  --s3-endpoint localhost:9000 \
  --s3-access-key your_access_key \
  --s3-secret-key your_secret_key \
  --s3-bucket your_bucket \
  --prefix postgres/backups \
  --pg-backup-type basebackup \
  --incremental \
  --interval 10
```

### Common Options

- `--type`: Database type (mongodb or postgresql)
- `--connection-string`: Database connection string
- `--host`: Database host
- `--port`: Database port
- `--username`: Database username
- `--password`: Database password
- `--s3-endpoint`: S3 endpoint URL
- `--s3-access-key`: S3 access key
- `--s3-secret-key`: S3 secret key
- `--s3-bucket`: S3 bucket name
- `--prefix`: Backup prefix/path in bucket
- `--incremental`: Enable point-in-time recovery capabilities
- `--interval`: Interval in minutes for continuous oplog/WAL archiving (default: 10)
- `--databases`: Specific databases to backup (comma-separated)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- MongoDB Team for MongoDB Tools
- PostgreSQL Team for PostgreSQL Client Tools
- AWS Team for S3 SDK 
