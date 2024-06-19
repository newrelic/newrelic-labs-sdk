package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	gIntegrationName    = "Labs SDK Example 2"
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

type ipify struct {
	IpAddress string `json:"ip"`
}

func newDecoder(num int) pipeline.LogsDecoderFunc {
	return func (
		receiver pipeline.LogsReceiver,
		in io.ReadCloser,
		out chan <- model.Log,
	) error {
		ip := ipify{}

		dec := json.NewDecoder(in)

		err := dec.Decode(&ip)
		if err != nil {
			return err
		}

		log.Debugf("receiver %d: my IP is %s", num, string(ip.IpAddress))

		iplog := model.NewLog(
			fmt.Sprintf("receiver %d: my IP is %s", num, string(ip.IpAddress)),
			map[string]interface{}{"ip": ip.IpAddress},
			time.Now(),
		)

		out <- iplog

		return nil
	}
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

	// Create a logs pipeline
	lp := pipeline.NewLogsPipeline()

	// Create some receivers and add them to the pipeline
	for i := 0; i < 10; i += 1 {
		ipifyReceiver := pipeline.NewSimpleReceiver(
			"ipify",
			"https://api.ipify.org/?format=json",
			pipeline.WithLogsDecoder(newDecoder(i)),
		)
		lp.AddReceiver(ipifyReceiver)
	}

	// Create the newrelic exporter and add it to the pipeline
	newRelicExporter := exporters.NewNewRelicExporter("newrelic", i)
	lp.AddExporter(newRelicExporter)

	// Register the pipeline with the integration
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
