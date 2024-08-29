package pipeline

import (
	"context"
	"io"
	"sync"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
)

type EventsReceiver interface {
	Component
	PollEvents(context context.Context, writer chan <- model.Event) error
}

type EventsDecoderFunc func(
	receiver EventsReceiver,
	in io.ReadCloser,
	out chan <- model.Event,
) error

type EventsExporter interface {
	Component
	ExportEvents(ctx context.Context, events []model.Event) error
}

type EventsPipeline pipeline[model.Event]

func NewEventsPipeline() *EventsPipeline {
	return &EventsPipeline{
		receivers: []Receiver[model.Event]{},
		processorList: &ProcessorList[model.Event]{},
		exporters: []Exporter[model.Event]{},
	}
}

func (p *EventsPipeline) AddReceiver(receiver EventsReceiver) {
	p.receivers = append(
		p.receivers,
		NewReceiverAdapter[model.Event](receiver.GetId(), receiver.PollEvents),
	)
}

func (p *EventsPipeline) AddProcessor(processor ProcessorFunc[model.Event]) {
	p.processorList.AddProcessor(processor)
}

func (p *EventsPipeline) AddExporter(exporter EventsExporter) {
	p.exporters = append(
		p.exporters,
		NewExporterAdapter(exporter.GetId(), exporter.ExportEvents),
	)
}

func (p *EventsPipeline) Execute(ctx context.Context) error {
	return execute(ctx, p.instances)
}

func (p *EventsPipeline) ExecuteSync(ctx context.Context) []error {
	return executeSync[model.Event](
		ctx,
		p.receivers,
		p.processorList,
		p.exporters,
	)
}

func (p *EventsPipeline) Start(ctx context.Context, wg *sync.WaitGroup) error {
	instances, err := start[model.Event](
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


func (p *EventsPipeline) Shutdown(ctx context.Context) error {
	return shutdown[model.Event](
		ctx,
		p.instances,
	)
}
