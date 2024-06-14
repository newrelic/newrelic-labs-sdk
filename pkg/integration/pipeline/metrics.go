package pipeline

import (
	"context"
	"io"
	"sync"

	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/model"
)

type MetricsReceiver interface {
	Component
	PollMetrics(context context.Context, writer chan <- model.Metric) error
}

type MetricsDecoderFunc func(
	receiver MetricsReceiver,
	in io.ReadCloser,
	out chan <- model.Metric,
) error

type MetricsExporter interface {
	Component
	ExportMetrics(ctx context.Context, metrics []model.Metric) error
}

type MetricsPipeline pipeline[model.Metric]

func NewMetricsPipeline() *MetricsPipeline {
	return &MetricsPipeline{
		InputChan: make(chan model.Metric),
		receivers: []Receiver[model.Metric]{},
		receiverChans: []chan struct{}{},
		processorList: &ProcessorList[model.Metric]{},
		exporters: []Exporter[model.Metric]{},
		exportChan: make(chan []model.Metric),
		resultChan: make(chan error),
	}
}

func (p *MetricsPipeline) AddReceiver(receiver MetricsReceiver) {
	p.receivers = append(
		p.receivers,
		NewReceiverAdapter[model.Metric](receiver.GetId(), receiver.PollMetrics),
	)
	p.receiverChans = append(p.receiverChans, make(chan struct{}))
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

func (p *MetricsPipeline) Execute(ctx context.Context) error {
	return execute(ctx, p.receiverChans)
}

func (p *MetricsPipeline) ExecuteSync(ctx context.Context) []error {
	return executeSync[model.Metric](
		ctx,
		p.receivers,
		p.processorList,
		p.exporters,
	)
}

func (p *MetricsPipeline) Start(ctx context.Context, wg *sync.WaitGroup) error {
	return start[model.Metric](
		ctx,
		wg,
		p.InputChan,
		p.receivers,
		p.receiverChans,
		p.processorList,
		p.exporters,
		p.exportChan,
		p.resultChan,
	)
}


func (p *MetricsPipeline) Shutdown(ctx context.Context) error {
	return shutdown[model.Metric](
		ctx,
		p.InputChan,
		p.receiverChans,
		p.exportChan,
		p.resultChan,
	)
}
