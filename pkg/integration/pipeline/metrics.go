package pipeline

import (
	"context"
	"io"
	"sync"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
)

type MetricsReceiver interface {
	GetId() string
	PollMetrics(context context.Context, writer chan <- model.Metric) error
}

type MetricsDecoderFunc func(
	receiver MetricsReceiver,
	in io.ReadCloser,
	out chan <- model.Metric,
) error

type MetricsExporter interface {
	GetId() string
	ExportMetrics(ctx context.Context, metrics []model.Metric) error
}

type MetricsPipeline pipeline[model.Metric]

func NewMetricsPipeline(id string) *MetricsPipeline {
	return &MetricsPipeline{
		id: id,
		receivers: []Receiver[model.Metric]{},
		processorList: &ProcessorList[model.Metric]{},
		exporters: []Exporter[model.Metric]{},
	}
}

func (p *MetricsPipeline) AddReceiver(receiver MetricsReceiver) {
	p.receivers = append(
		p.receivers,
		NewReceiverAdapter[model.Metric](receiver.GetId(), receiver.PollMetrics),
	)
}

func (p *MetricsPipeline) AddProcessor(processor ProcessorFunc[model.Metric]) {
	p.processorList.AddProcessor(processor)
}

func (p *MetricsPipeline) AddExporter(exporter MetricsExporter) {
	p.exporters = append(
		p.exporters,
		NewExporterAdapter(exporter.GetId(), exporter.ExportMetrics),
	)
}

func (p *MetricsPipeline) GetId() string {
	return p.id
}

func (p *MetricsPipeline) Execute(ctx context.Context) error {
	return execute(ctx, p.instances)
}

func (p *MetricsPipeline) ExecuteSync(ctx context.Context) error {
	return executeSync[model.Metric](
		ctx,
		p.receivers,
		p.processorList,
		p.exporters,
	)
}

func (p *MetricsPipeline) Start(ctx context.Context, wg *sync.WaitGroup) error {
	instances, err := start[model.Metric](
		ctx,
		wg,
		p.receivers,
		p.processorList,
		p.exporters,
	)
	if err != nil {
		return err
	}

	p.instances = instances

	return nil
}


func (p *MetricsPipeline) Shutdown(ctx context.Context) error {
	return shutdown[model.Metric](
		ctx,
		p.instances,
	)
}
