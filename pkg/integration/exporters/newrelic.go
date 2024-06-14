package exporters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	nrClient "github.com/newrelic/newrelic-client-go/newrelic"
	"github.com/newrelic/newrelic-client-go/pkg/region"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/model"
)

type NewRelicExporter struct {
	id				string
	buildInfo		*integration.BuildInfo
	NrClient      	*nrClient.NewRelic
	licenseKey		string
	metricsUrl		string
	dryRun			bool
}

func NewNewRelicExporter(id string, li *integration.LabsIntegration) *NewRelicExporter {
	metricsUrl := getMetricsUrl(li.GetRegion())

	return &NewRelicExporter{
		id,
		li.BuildInfo,
		li.NrClient,
		li.GetLicenseKey(),
		metricsUrl,
		li.DryRun,
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

	NRPayload := newMetricsPayload(nrMetrics, e.buildInfo)

	json, err := json.Marshal([]interface{}{ NRPayload })
	if err != nil {
		return err
	}

	if e.dryRun || log.IsDebugEnabled() {
		log.Debugf("metrics payload JSON follows")
		log.Debugf("%s", string(json))

		if e.dryRun {
			return nil
		}
	}

	err = e.postMetrics(json)
	if err != nil {
		return err
	}

	return nil
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

		evt["instrumentation.name"] = e.buildInfo.Name
		evt["instrumentation.provider"] = "newrelic-labs"
		evt["instrumentation.version"] = e.buildInfo.Version
		evt["collector.name"] = e.buildInfo.Id

		if e.dryRun || log.IsDebugEnabled() {
			log.Debugf("event payload JSON follows")
			log.PrettyPrintJson(evt)

			if e.dryRun {
				continue
			}
		}

		if err := e.NrClient.Events.EnqueueEvent(ctx, evt); err != nil {
			return fmt.Errorf("failed to enqueue event: %w", err)
		}
	}

	log.Debugf("all events enqueued; flushing events")

	return e.NrClient.Events.Flush()
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

		logEntry.Attributes["instrumentation.name"] = e.buildInfo.Name
		logEntry.Attributes["instrumentation.provider"] = "newrelic-labs"
		logEntry.Attributes["instrumentation.version"] = e.buildInfo.Version
		logEntry.Attributes["collector.name"] = e.buildInfo.Id

		if e.dryRun || log.IsDebugEnabled(){
			log.Debugf("log payload JSON follows")
			log.PrettyPrintJson(logEntry)

			if e.dryRun {
				continue
			}
		}

		if err := e.NrClient.Logs.EnqueueLogEntry(ctx, logEntry); err != nil {
			return fmt.Errorf("failed to enqueue log: %w", err)
		}
	}

	log.Debugf("all logs enqueued; flushing logs")

	return e.NrClient.Events.Flush()
}

type NRLogEntry struct {
	Message string						`json:"message"`
	Timestamp int64						`json:"timestamp"`
	Attributes map[string]interface{}	`json:"attributes"`
}

// based on https://github.com/atlassian/gostatsd/blob/master/pkg/backends/newrelic/newrelic.go

// NRMetricsPayload represents New Relic Metrics Payload format
// https://docs.newrelic.com/docs/data-ingest-apis/get-data-new-relic/metric-api/report-metrics-metric-api#new-relic-guidelines
type NRMetricsPayload struct {
	Common  NRMetricsCommon `json:"common"`
	Metrics []NRMetric   	`json:"metrics"`
}

// NRMetricsCommon common attributes to apply for New Relic Metrics Format
type NRMetricsCommon struct {
	Attributes map[string]interface{} `json:"attributes"`
}

// NRMetric metric for New Relic Metrics Format
type NRMetric struct {
	Name       string                 `json:"name"`
	Value      interface{}            `json:"value,omitempty"`
	Type       string                 `json:"type,omitempty"`
	Timestamp  int64                  `json:"timestamp,omitempty"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	IntervalMs float64                `json:"interval.ms,omitempty"`
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

func newMetricsPayload(nrMetrics []NRMetric, buildInfo *integration.BuildInfo) NRMetricsPayload {
	return NRMetricsPayload{
		Common: NRMetricsCommon{
			Attributes: map[string]interface{}{
				"instrumentation.name": buildInfo.Name,
				"instrumentation.provider": "newrelic-labs",
				"instrumentation.version": buildInfo.Version,
				"collector.name": buildInfo.Id,
			},
		},
		Metrics: nrMetrics,
	}
}

func (e *NewRelicExporter) postMetrics(json []byte) error {
	headers := map[string]string{
		"Content-Type": "application/json",
		"Api-Key": e.licenseKey,
	}

	req, err := http.NewRequest("POST", e.metricsUrl, bytes.NewBuffer(json))
	if err != nil {
		return fmt.Errorf("unable to create http.Request: %w", err)
	}

	for header, v := range headers {
		req.Header.Set(header, v)
	}

	log.Debugf("posting metrics to New Relic endpoint %s", e.metricsUrl)

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error posting: %s", err.Error())
	}

	defer resp.Body.Close()

	log.Debugf("http status code received: %d", resp.StatusCode)

	if (log.IsDebugEnabled()) {
		log.Debugf("metrics response JSON follows")

		body, _ := io.ReadAll(resp.Body)
		log.Debugf("%s", string(body))
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusNoContent {
		return fmt.Errorf("received bad status code %d", resp.StatusCode)
	}

	return nil
}

func getMetricsUrl(nrRegion region.Name) string {
	if nrRegion == region.EU {
		return "https://metric-api.eu.newrelic.com/metric/v1"
	}

	return "https://metric-api.newrelic.com/metric/v1"
}
