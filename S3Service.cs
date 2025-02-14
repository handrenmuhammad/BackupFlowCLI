using Amazon.S3;
using Amazon.S3.Model;
using Spectre.Console;

namespace DbBackupCLI;

public class S3Service
{
    internal readonly AmazonS3Client _s3Client;

    public S3Service(string s3Endpoint, string s3AccessKey, string s3SecretKey)
    {
        var s3Config = new AmazonS3Config
        {
            ServiceURL = $"http://{s3Endpoint}",
            ForcePathStyle = true
        };
        _s3Client = new AmazonS3Client(s3AccessKey, s3SecretKey, s3Config);
    }

    public async Task EnsureBucketExistsAsync(string bucketName)
    {
        try
        {
            var bucketExists = await _s3Client.ListBucketsAsync();
            if (!bucketExists.Buckets.Any(b => b.BucketName == bucketName))
            {
                var putBucketRequest = new PutBucketRequest
                {
                    BucketName = bucketName
                };
                await _s3Client.PutBucketAsync(putBucketRequest);
                AnsiConsole.MarkupLine($"[green] Created S3 bucket: {bucketName}[/]");
            }
        }
        catch (AmazonS3Exception ex)
        {
            throw new Exception($"failed to create S3 bucket: {ex.Message}", ex);
        }
    }

    public async Task<ListObjectsV2Response> ListObjectsAsync(string bucketName, string prefix)
    {
        var request = new ListObjectsV2Request { BucketName = bucketName, Prefix = prefix };
        var response = await _s3Client.ListObjectsV2Async(request);
        return response;
    }

    public async Task DownloadObjectAsync(string bucketName, string key, string filePath)
    {
        var request = new GetObjectRequest { BucketName = bucketName, Key = key };
        using var response = await _s3Client.GetObjectAsync(request);
        await using var fileStream = File.OpenWrite(filePath);
        await response.ResponseStream.CopyToAsync(fileStream);
    }
}
