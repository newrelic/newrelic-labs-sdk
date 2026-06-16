package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/exporters"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/pipeline"
)

const (
	INTEGRATION_ID = "com.newrelic.labs.sdk.test2"
	INTEGRATION_NAME = "Labs SDK Example 2"
)

type echo struct {
	Method string `json:"method"`
	Protocol string `json:"protocol"`
	Host string `json:"hostname"`
	Path string `json:"path"`
	Headers map[string]string `json:"headers"`
}

func newDecoder() pipeline.LogsDecoderFunc {
	return func (
		receiver pipeline.LogsReceiver,
		in io.ReadCloser,
		out chan <- model.Log,
	) error {
		e := echo{}

		dec := json.NewDecoder(in)

		err := dec.Decode(&e)
		if err != nil {
			return err
		}

		out <- model.NewLog(
			fmt.Sprintf(
				"method: %s: protocol: %s: host: %s: path: %s",
				e.Method,
				e.Protocol,
				e.Host,
				e.Path,
			),
			nil,
			time.Now(),
		)

		userAgent, ok := e.Headers["user-agent"]
		if ok {
			out <- model.NewLog(
				fmt.Sprintf(
					"User-Agent: %s",
					userAgent,
				),
				nil,
				time.Now(),
			)
		}

		accept, ok := e.Headers["accept"]
		if ok {
			out <- model.NewLog(
				fmt.Sprintf(
					"Accept: %s",
					accept,
				),
				nil,
				time.Now(),
			)
		}

		contentType, ok := e.Headers["content-type"]
		if ok {
			out <- model.NewLog(
				fmt.Sprintf(
					"Content-Type: %s",
					contentType,
				),
				nil,
				time.Now(),
			)
		}

		return nil
	}
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
	)
	fatalIfErr(err)

	// Create a logs pipeline
	lp := pipeline.NewLogsPipeline("echo-header-logs-pipeline")

	// Create some receivers and add them to the pipeline
	echoReceiver := pipeline.NewSimpleReceiver(
			"echo-receiver",
			"http://localhost:8080",
			pipeline.WithLogsDecoder(newDecoder()),
		)
	lp.AddReceiver(echoReceiver)

	// Create the newrelic exporter and add it to the pipeline
	newRelicExporter := exporters.NewNewRelicExporter(
		"newrelic",
		INTEGRATION_NAME,
		INTEGRATION_ID,
		i.NrClient,
		i.GetLicenseKey(),
		i.GetRegion(),
		i.DryRun,
	)
	lp.AddExporter(newRelicExporter)

	// Register the pipeline with the integration
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
