package main

import (
	"context"
	"fmt"
	"runtime/metrics"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/exporters"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/pipeline"
)

const (
	INTEGRATION_ID = "com.newrelic.labs.sdk.test4"
	INTEGRATION_NAME = "Labs SDK Example 4"
)

type runtimeMetricReceiver struct {
}

func (t *runtimeMetricReceiver) GetId() string {
	return "runtime-metric-receiver"
}

// This shows how you can manually generate metrics by implementing PollMetrics
// directly on a component instead of going through one of the HTTP receivers.
func (t *runtimeMetricReceiver) PollMetrics(
	ctx context.Context,
	writer chan <- model.Metric,
) error {
	// Example taken from https://pkg.go.dev/runtime/metrics@go1.23.1#example-Read-ReadingOneMetric

	// Logging is automatically configured and available to you
	log.Debugf("generating test SDK metrics")

	const myMetric = "/memory/classes/heap/free:bytes"

	sample := make([]metrics.Sample, 1)
	sample[0].Name = myMetric

	metrics.Read(sample)

	if sample[0].Value.Kind() == metrics.KindBad {
		return fmt.Errorf("metric %q no longer supported", myMetric)
	}

	freeBytes := sample[0].Value.Uint64()

	writer <- model.NewGaugeMetric(
		"labs.sdk.test.memory.heapFree",
		model.MakeNumeric(int64(freeBytes)),
		time.Now(),
	)

	return nil
}

func main() {
	// Create a new background context to use
	ctx := context.Background()

	// Create the integration with options
	i, err := integration.NewStandaloneIntegration(
		INTEGRATION_NAME,
		INTEGRATION_ID,
		INTEGRATION_NAME,
		integration.WithApiKey(),
		integration.WithLicenseKey(),
		integration.WithLogs(ctx),
		integration.WithLastUpdate(),
	)
	fatalIfErr(err)

	// Create application components (receivers, processors, exporters)
	component := &runtimeMetricReceiver{}
	newRelicExporter := exporters.NewNewRelicExporter(
		"newrelic",
		INTEGRATION_NAME,
		INTEGRATION_ID,
		i.NrClient,
		i.GetLicenseKey(),
		i.GetRegion(),
		i.DryRun,
	)

	// Create one or more pipelines
	mp := pipeline.NewMetricsPipeline("runtime-metric-metrics-pipeline")
	mp.AddReceiver(component)
	mp.AddExporter(newRelicExporter)

	// Register a schedule with the integration to run the pipeline component
	// every minute
	i.AddSchedule("* * * * *", []integration.Component{ mp })

	// Add component
	component2 := integration.NewSimpleComponent(
		"test-last-update-component",
		i,
		func(ctx context.Context, li *integration.LabsIntegration) error {
			return li.UseLastUpdate(ctx, func(lastUpdate time.Time) error {
				log.Infof("last update is %v", lastUpdate)
				return nil
			}, "test-last-update-component")
		},
	)

	i.AddComponent(component2)

	// Run the integration
	defer i.Shutdown(ctx)
 	err = i.Run(ctx)
	fatalIfErr(err)
}

func fatalIfErr(err error) {
	if err != nil {
		log.Fatalf(err)
	}
}
