package exporters

import (
	"context"
	"fmt"
	"strconv"
	"time"

	nriSdkMetrics "github.com/newrelic/infra-integrations-sdk/v4/data/metric"
	nriSdk "github.com/newrelic/infra-integrations-sdk/v4/integration"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/model"
)

type NewRelicInfraExporter struct {
	id				string
	buildInfo		*integration.BuildInfo
	i 				*nriSdk.Integration
	entity			*nriSdk.Entity
}

func NewNewRelicInfraExporter(id string, li *integration.LabsIntegration) *NewRelicInfraExporter {
	return &NewRelicInfraExporter{
		id,
		li.BuildInfo,
		li.Integration,
		nil,
	}
}

func (e *NewRelicInfraExporter) GetId() string {
	return e.id
}

func (e *NewRelicInfraExporter) ExportMetrics(
	ctx context.Context,
	metrics []model.Metric,
) error {
	log.Debugf("adding metrics to entity")

	for i := 0; i < len(metrics); i += 1 {
		metric := metrics[i]

		switch metric.Type {
		case model.Gauge:
			gauge, err := nriSdk.Gauge(
				time.UnixMilli(metric.Timestamp),
				metric.Name,
				metric.Value.Float(),
			)
			if err != nil {
				return fmt.Errorf("error creating gauge metric: %w", err)
			}

			e.addAttributes(&metric, gauge)
			e.entity.AddMetric(gauge)

		case model.Count:
			// @TODO: NO TIME INTERVAL???
			count, err := nriSdk.Count(
				time.UnixMilli(metric.Timestamp),
				metric.Name,
				metric.Value.Float(),
			)
			if err != nil {
				return fmt.Errorf("error creating count metric: %w", err)
			}

			e.addAttributes(&metric, count)
			e.entity.AddMetric(count)

		case model.Summary:
			// @TODO
		}
	}

	return nil
}

func (e *NewRelicInfraExporter) ExportEvents(
	ctx context.Context,
	events []model.Event,
) error {
	return fmt.Errorf("the new relic infrastructure exporter does not support exporting New Relic Events data")
}

func (e *NewRelicInfraExporter) ExportLogs(
	ctx context.Context,
	logs []model.Log,
) error {
	return fmt.Errorf("the new relic infrastructure exporter does not support exporting New Relic Logs data")
}

func (e *NewRelicInfraExporter) addAttributes(metric *model.Metric, nriMetric nriSdkMetrics.Metric) {

	nriMetric.AddDimension("instrumentation.name", e.buildInfo.Name)
	nriMetric.AddDimension("instrumentation.provider", "newrelic-labs")
	nriMetric.AddDimension("instrumentation.version", e.buildInfo.Version)
	nriMetric.AddDimension("collector.name", e.buildInfo.Id)

	for k, v := range metric.Attributes {
		switch val := v.(type) {
		case string:
			nriMetric.AddDimension(k, val)
		case int:
			nriMetric.AddDimension(k, strconv.Itoa(val))
		case float32:
			nriMetric.AddDimension(k, strconv.FormatFloat(float64(val), 'f', 2, 32))
		case float64:
			nriMetric.AddDimension(k, strconv.FormatFloat(val, 'f', 2, 32))
		case fmt.Stringer:
			nriMetric.AddDimension(k, val.String())
		default:
			log.Warnf("metric attribute of unsupported type: %s: %v", k, v)
		}

	}
}
