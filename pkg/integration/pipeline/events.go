package pipeline

import (
	"context"
	"io"
	"sync"

	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/model"
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
		InputChan: make(chan model.Event),
		receivers: []Receiver[model.Event]{},
		receiverChans: []chan struct{}{},
		processorList: &ProcessorList[model.Event]{},
		exporters: []Exporter[model.Event]{},
		exportChan: make(chan []model.Event),
		resultChan: make(chan error),
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
	return execute(ctx, p.receiverChans)
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
	return start[model.Event](
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


func (p *EventsPipeline) Shutdown(ctx context.Context) error {
	return shutdown[model.Event](
		ctx,
		p.InputChan,
		p.receiverChans,
		p.exportChan,
		p.resultChan,
	)
}
