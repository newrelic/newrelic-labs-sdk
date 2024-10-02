package exporters

import (
	"encoding/json"
	"io"

	"github.com/newrelic/newrelic-client-go/v2/pkg/region"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/build"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/connectors"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
)

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

type NewRelicMetricsClient struct {
	integrationName	string
	integrationId	string
	buildInfo		build.BuildInfo
	licenseKey		string
	client			*connectors.HttpConnector
	dryRun			bool
}

func NewNewRelicMetricsClient(
	integrationName, integrationId string,
	licenseKey string,
	region region.Name,
	dryRun bool,
) *NewRelicMetricsClient {
	metricsUrl := getMetricsUrl(region)

	return &NewRelicMetricsClient{
		integrationName,
		integrationId,
		build.GetBuildInfo(),
		licenseKey,
		connectors.NewHttpPostConnector(metricsUrl, nil),
		dryRun,
	}
}

func (c *NewRelicMetricsClient) PostMetrics(
	metrics []NRMetric,
) error {
	NRPayload := c.newMetricsPayload(metrics)

	json, err := json.Marshal([]interface{}{ NRPayload })
	if err != nil {
		return err
	}

	if c.dryRun || log.IsDebugEnabled() {
		log.Debugf("metrics payload JSON follows")
		log.PrettyPrintJson(NRPayload)

		if c.dryRun {
			return nil
		}
	}

	headers := map[string]string{
		"Accept": "application/json",
		"Content-Type": "application/json",
		"Api-Key": c.licenseKey,
	}

	c.client.SetHeaders(headers)
	c.client.SetBody(json)

	log.Debugf("posting metrics to New Relic endpoint")

	readCloser, err := c.client.Request()
	if err != nil {
		return err
	}

	defer readCloser.Close()

	if log.IsDebugEnabled() {
		log.Debugf("metrics response payload follows")

		bytes, err := io.ReadAll(readCloser)
		if err != nil {
			log.Warnf("error reading metrics response: %v", err)
		} else {
			log.Debugf(string(bytes))
		}
	}

	return nil
}

func (c *NewRelicMetricsClient) newMetricsPayload(
	nrMetrics []NRMetric,
) *NRMetricsPayload {
	return &NRMetricsPayload{
		Common: NRMetricsCommon{
			Attributes: map[string]interface{}{
				"instrumentation.name": c.integrationName,
				"instrumentation.provider": "newrelic-labs",
				"instrumentation.version": c.buildInfo.Version,
				"collector.name": c.integrationId,
			},
		},
		Metrics: nrMetrics,
	}
}

func getMetricsUrl(nrRegion region.Name) string {
	if nrRegion == region.EU {
		return "https://metric-api.eu.newrelic.com/metric/v1"
	}

	return "https://metric-api.newrelic.com/metric/v1"
}
