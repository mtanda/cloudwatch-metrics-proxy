package cloudwatch

import (
	"context"
	"math"
	"os"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

var invalidMetricNamePattern = regexp.MustCompile(`[^a-zA-Z0-9:_]`)

func safeMetricName(name string) string {
	if len(name) == 0 {
		return ""
	}
	name = invalidMetricNamePattern.ReplaceAllString(name, "_")
	if '0' <= name[0] && name[0] <= '9' {
		name = "_" + name
	}
	return name
}

func isSingleStatistic(queries []*cloudwatch.GetMetricStatisticsInput) bool {
	s := ""
	for _, query := range queries {
		if len(query.Statistics) > 1 || len(query.ExtendedStatistics) > 1 {
			return false
		}
		if len(query.Statistics) == 1 {
			if s == "" {
				s = string(query.Statistics[0])
				continue
			}
			if s != string(query.Statistics[0]) {
				return false
			}
		}
		if len(query.ExtendedStatistics) == 1 {
			if s == "" {
				s = query.ExtendedStatistics[0]
				continue
			}
			if s != query.ExtendedStatistics[0] {
				return false
			}
		}
	}
	return true
}

func isExtendedStatistics(s string) bool {
	return s != "Sum" && s != "SampleCount" && s != "Maximum" && s != "Minimum" && s != "Average"
}

func calcQueryPeriod(startTime time.Time, endTime time.Time, periodUnit int64) *int32 {
	period := calibratePeriod(startTime)
	queryTimeRange := (endTime).Sub(startTime).Seconds()
	if queryTimeRange/float64(period) >= 1440 {
		period = int64(math.Ceil(queryTimeRange/float64(1440)/float64(periodUnit))) * int64(periodUnit)
	}
	return aws.Int32(int32(period))
}

func calibratePeriod(startTime time.Time) int64 {
	var period int64

	timeDay := 24 * time.Hour
	now := time.Now().UTC()
	timeRangeToNow := now.Sub(startTime)
	if timeRangeToNow < timeDay*15 { // until 15 days ago
		period = 60
	} else if timeRangeToNow <= (timeDay * 63) { // until 63 days ago
		period = 60 * 5
	} else if timeRangeToNow <= (timeDay * 455) { // until 455 days ago
		period = 60 * 60
	} else { // over 455 days, should return error, but try to long period
		period = 60 * 60
	}

	return period
}

var regionCache = ""

func getDefaultRegion() (string, error) {
	var region string

	if regionCache != "" {
		return regionCache, nil
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
	}

	client := imds.NewFromConfig(cfg)
	response, err := client.GetRegion(ctx, &imds.GetRegionInput{})
	if err != nil {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
	} else {
		region = response.Region
		if region != "" {
			regionCache = region
		}
	}

	return region, nil
}

func getClient(ctx context.Context, region string) (*cloudwatch.Client, error) {
	if client, ok := clientCache[region]; ok {
		return client, nil
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	clientCache[region] = cloudwatch.NewFromConfig(awsCfg)
	return clientCache[region], nil
}
