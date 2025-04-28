package main

import (
	"context"
	"os"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
)

var invalidMetricNamePattern = regexp.MustCompile(`[^a-zA-Z0-9:_]`)

func isExtendedStatistics(s string) bool {
	return s != "Sum" && s != "SampleCount" && s != "Maximum" && s != "Minimum" && s != "Average"
}

var regionCache = ""

func GetDefaultRegion() (string, error) {
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

func SafeMetricName(name string) string {
	if len(name) == 0 {
		return ""
	}
	name = invalidMetricNamePattern.ReplaceAllString(name, "_")
	if '0' <= name[0] && name[0] <= '9' {
		name = "_" + name
	}
	return name
}
