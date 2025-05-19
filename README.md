# cloudwatch-metrics-proxy

`cloudwatch-metrics-proxy` is a remote read adapter for Prometheus, enabling you to query AWS CloudWatch metrics from Prometheus using the remote_read feature.

## Build

### Local build

```sh
go build ./cmd/cloudwatch_metrics_proxy
```

### Package build

```sh
# For release
goreleaser --clean

# For snapshot
goreleaser --snapshot --clean
```

## Usage

### Prometheus Configuration Example

Add the following to your `prometheus.yml`:

```yaml
remote_read:
  - url: http://localhost:9420/read
```

### How to Start

**Note:** `cloudwatch-metrics-proxy` depends on `prometheus-labels-db`. Please make sure to start `prometheus-labels-db` before launching this proxy.

```sh
./cloudwatch_metrics_proxy
```

### Query Example

Specify CloudWatch GetMetricStatistics parameters as labels:

```text
{Region="us-east-1",Namespace="AWS/EC2",MetricName="CPUUtilization",InstanceId="i-0123456789abcdefg",Statistics="Average",Period="300"}
```

*Note: The `Period` parameter is optional. If omitted, it will be automatically adjusted.*

## AWS Credentials

This adapter uses the AWS SDK for Go, so it obtains credentials from environment variables, IAM roles for Amazon EC2, and other standard methods. For details, see the [official documentation](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html).

## Testing

```sh
go test ./...
```
