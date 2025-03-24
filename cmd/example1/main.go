package main

import (
	"context"
	"fmt"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/exporters"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/pipeline"
)

const (
	INTEGRATION_ID = "com.newrelic.labs.sdk.test1"
	INTEGRATION_NAME = "Labs SDK Example 1"
)

type testReceiver struct {
}

func (t *testReceiver) GetId() string {
	return "test-receiver"
}

// This shows how you can manually generate metrics by implementing PollMetrics
// directly on a component instead of going through one of the HTTP receivers.
func (t *testReceiver) PollMetrics(ctx context.Context, writer chan <- model.Metric) error {
	// Logging is automatically configured and available to you
	log.Debugf("generating test SDK metrics")

	for i := 0; i < 100; i += 1 {
		writer <- model.NewGaugeMetric(
			fmt.Sprintf("labs.sdk.test.%d", i),
			model.MakeNumeric(i),
			time.Now(),
		)
	}

	return nil
}

// This shows how you can manually generate events by implementing PollEvents
// directly on a component instead of going through one of the HTTP receivers.
func (t *testReceiver) PollEvents(ctx context.Context, writer chan <- model.Event) error {
	// Logging is automatically configured and available to you
	log.Debugf("generating test FibonnaciNumber events")

	a := 0
	b := 1

	writer <- model.NewEvent("FibonacciNumber", map[string]any { "Num": a }, time.Now())
	writer <- model.NewEvent("FibonacciNumber", map[string]any { "Num": b }, time.Now())

	for i := 0; i < 50; i += 1 {
		c := a + b
		a = b
		b = c

		writer <- model.NewEvent("FibonacciNumber", map[string]any { "Num": c }, time.Now())
	}

	return nil
}

// This shows how you can manually generate log by implementing PollLogs
// directly on a component instead of going through one of the HTTP receivers.
func (t *testReceiver) PollLogs(ctx context.Context, writer chan <- model.Log) error {
	// Logging is automatically configured and available to you
	log.Debugf("generating test logs")

	for i := 0; i < 100; i += 1 {
		writer <- model.NewLog(
			fmt.Sprintf("Lorem ipsum dolor sit amet %d", i),
			map[string]any { "Foo": "Bar", "Beep": "Boop" },
			time.Now(),
		)
	}

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
		integration.WithLicenseKey(),
		integration.WithApiKey(),
		integration.WithAccountId(),
		integration.WithEvents(ctx),
		integration.WithLogs(ctx),
	)
	fatalIfErr(err)

	// Create application components (receivers, processors, exporters)
	component := &testReceiver{}
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
	mp := pipeline.NewMetricsPipeline("example-metrics-pipeline")
	mp.AddReceiver(component)
	mp.AddExporter(newRelicExporter)

	ep := pipeline.NewEventsPipeline("example-events-pipeline")
	ep.AddReceiver(component)
	ep.AddExporter(newRelicExporter)

	lp := pipeline.NewLogsPipeline("example-logs-pipeline")
	lp.AddReceiver(component)
	lp.AddExporter(newRelicExporter)

	// Register the pipelines with the integration
	i.AddComponent(mp)
	i.AddComponent(ep)
	i.AddComponent(lp)

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
