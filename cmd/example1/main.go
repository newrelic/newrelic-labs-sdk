package main

import (
	"context"
	"fmt"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/exporters"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/model"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/pipeline"
)

var (
	/* Args below are populated via ldflags at build time */
	gIntegrationID      = "com.newrelic.labs.test"
	gIntegrationName    = "Labs SDK Example 1"
	gIntegrationVersion = "0.1.0"
	gGitCommit          = ""
	gBuildDate          = ""
	gBuildInfo			= integration.BuildInfo{
		Id:        gIntegrationID,
		Name:      gIntegrationName,
		Version:   gIntegrationVersion,
		GitCommit: gGitCommit,
		BuildDate: gBuildDate,
	}
)

type testComponent2 struct {
}

func (t *testComponent2) GetId() string {
	return "test-component-2"
}

// This shows how you can manually generate metrics by implementing PollMetrics
// directly on a component instead of going through one of the HTTP receivers.
func (t *testComponent2) PollMetrics(ctx context.Context, writer chan <- model.Metric) error {
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
func (t *testComponent2) PollEvents(ctx context.Context, writer chan <- model.Event) error {
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
func (t *testComponent2) PollLogs(ctx context.Context, writer chan <- model.Log) error {
	// Logging is automatically configured and available to you
	log.Debugf("generating test logs")

	for i := 0; i < 100; i += 1 {
		writer <- model.NewLog(
			"Lorem ipsum dolor sit amet",
			map[string]any { "Foo": "Bar", "Beep": "Boop" },
			time.Now(),
		)
	}

	return nil
}

func main() {
	// Create a new background context to us
	ctx := context.Background()

	// Create the integration with options
	i, err := integration.NewStandaloneIntegration(
		&gBuildInfo,
		gBuildInfo.Name,
		integration.WithLicenseKey(),
		integration.WithApiKey(),
		integration.WithAccountId(),
		integration.WithClient(),
		integration.WithEvents(ctx),
		integration.WithLogs(ctx),
	)
	fatalIfErr(err)

	// Create application components (receivers, processors, exporters)
	component := &testComponent2{}
	newRelicExporter := exporters.NewNewRelicExporter("newrelic", i)

	// Create one or more pipelines
	mp := pipeline.NewMetricsPipeline()
	mp.AddReceiver(component)
	mp.AddExporter(newRelicExporter)

	ep := pipeline.NewEventsPipeline()
	ep.AddReceiver(component)
	ep.AddExporter(newRelicExporter)

	lp := pipeline.NewLogsPipeline()
	lp.AddReceiver(component)
	lp.AddExporter(newRelicExporter)

	// Register the pipelines with the integration
	i.AddPipeline(mp)
	i.AddPipeline(ep)
	i.AddPipeline(lp)

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
