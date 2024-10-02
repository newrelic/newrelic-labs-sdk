package exporters

import (
	"context"
	"fmt"

	nrClient "github.com/newrelic/newrelic-client-go/v2/newrelic"
	"github.com/newrelic/newrelic-client-go/v2/pkg/region"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/build"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
)

type NewRelicExporter struct {
	id				string
	integrationName	string
	integrationId	string
	buildInfo		build.BuildInfo
	nrClient      	*nrClient.NewRelic
	metricsClient	*NewRelicMetricsClient
	licenseKey		string
	dryRun			bool
}

func NewNewRelicExporter(
	id, integrationName, integrationId string,
	nrClient *nrClient.NewRelic,
	licenseKey string,
	region region.Name,
	dryRun bool,
) *NewRelicExporter {
	return &NewRelicExporter{
		id,
		integrationName,
		integrationId,
		build.GetBuildInfo(),
		nrClient,
		NewNewRelicMetricsClient(
			integrationName,
			integrationId,
			licenseKey,
			region,
			dryRun,
		),
		licenseKey,
		dryRun,
	}
}

func (e *NewRelicExporter) GetId() string {
	return e.id
}

func (e *NewRelicExporter) ExportMetrics(
	ctx context.Context,
	metrics []model.Metric,
) error {
	log.Debugf("exporting metrics to New Relic")

	nrMetrics, err := processMetrics(metrics)
	if err != nil {
		return err
	}

	return e.metricsClient.PostMetrics(nrMetrics)
}

func (e *NewRelicExporter) ExportEvents(
	ctx context.Context,
	events []model.Event,
) error {
	log.Debugf("exporting events to New Relic")

	for _, event := range events {
		evt := map[string]interface{}{}

		evt["eventType"] = event.Type

		for k, v := range event.Attributes {
			evt[k] = v
		}

		evt["timestamp"] = event.Timestamp

		evt["instrumentation.name"] = e.integrationName
		evt["instrumentation.provider"] = "newrelic-labs"
		evt["instrumentation.version"] = e.buildInfo.Version
		evt["collector.name"] = e.integrationId

		if e.dryRun || log.IsDebugEnabled() {
			log.Debugf("event payload JSON follows")
			log.PrettyPrintJson(evt)

			if e.dryRun {
				continue
			}
		}

		if err := e.nrClient.Events.EnqueueEvent(ctx, evt); err != nil {
			return fmt.Errorf("failed to enqueue event: %w", err)
		}
	}

	log.Debugf("all events enqueued; flushing events")

	return e.nrClient.Events.Flush()
}

func (e *NewRelicExporter) ExportLogs(
	ctx context.Context,
	logs []model.Log,
) error {
	log.Debugf("exporting logs to New Relic")

	for _, l := range logs {
		logEntry := NRLogEntry{
			l.Message,
			l.Timestamp,
			l.Attributes,
		}

		logEntry.Attributes["instrumentation.name"] = e.integrationName
		logEntry.Attributes["instrumentation.provider"] = "newrelic-labs"
		logEntry.Attributes["instrumentation.version"] = e.buildInfo.Version
		logEntry.Attributes["collector.name"] = e.integrationId

		if e.dryRun || log.IsDebugEnabled(){
			log.Debugf("log payload JSON follows")
			log.PrettyPrintJson(logEntry)

			if e.dryRun {
				continue
			}
		}

		if err := e.nrClient.Logs.EnqueueLogEntry(ctx, logEntry); err != nil {
			return fmt.Errorf("failed to enqueue log: %w", err)
		}
	}

	log.Debugf("all logs enqueued; flushing logs")

	return e.nrClient.Logs.Flush()
}

type NRLogEntry struct {
	Message string						`json:"message"`
	Timestamp int64						`json:"timestamp"`
	Attributes map[string]interface{}	`json:"attributes"`
}

func processMetrics(metrics []model.Metric) ([]NRMetric, error) {
	nrMetrics := []NRMetric{}

	for i := 0; i < len(metrics); i += 1 {
		metric := metrics[i]

		var nrMetric NRMetric

		switch metric.Type {
		case model.Gauge:
			nrMetric.Name = metric.Name
			nrMetric.Value = metric.Value.Value()
			nrMetric.Type = "gauge"
			nrMetric.Timestamp = metric.Timestamp
			nrMetric.Attributes = metric.Attributes

		case model.Count:
			nrMetric.Name = metric.Name
			nrMetric.Value = metric.Value.Value()
			nrMetric.Type = "count"
			nrMetric.Timestamp = metric.Timestamp
			nrMetric.Attributes = metric.Attributes
			nrMetric.IntervalMs = float64(metric.Interval.Milliseconds())

		case model.Summary:
			// @TODO implement summary metrics
		}

		nrMetrics = append(nrMetrics, nrMetric)
	}

	return nrMetrics, nil
}
