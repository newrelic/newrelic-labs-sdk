package pipeline

import (
	"context"
	"io"
	"sync"

	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/model"
)

type LogsReceiver interface {
	Component
	PollLogs(context context.Context, writer chan <- model.Log) error
}

type LogsDecoderFunc func(
	receiver LogsReceiver,
	in io.ReadCloser,
	out chan <- model.Log,
) error

type LogsExporter interface {
	Component
	ExportLogs(ctx context.Context, logs []model.Log) error
}

type LogsPipeline pipeline[model.Log]

func NewLogsPipeline() *LogsPipeline {
	return &LogsPipeline{
		InputChan: make(chan model.Log),
		receivers: []Receiver[model.Log]{},
		receiverChans: []chan struct{}{},
		processorList: &ProcessorList[model.Log]{},
		exporters: []Exporter[model.Log]{},
		exportChan: make(chan []model.Log),
		resultChan: make(chan error),
	}
}

func (p *LogsPipeline) AddReceiver(receiver LogsReceiver) {
	p.receivers = append(
		p.receivers,
		NewReceiverAdapter[model.Log](receiver.GetId(), receiver.PollLogs),
	)
}

func (p *LogsPipeline) AddProcessor(processor ProcessorFunc[model.Log]) {
	p.processorList.AddProcessor(processor)
}

func (p *LogsPipeline) AddExporter(exporter LogsExporter) {
	p.exporters = append(
		p.exporters,
		NewExporterAdapter(exporter.GetId(), exporter.ExportLogs),
	)
}

func (p *LogsPipeline) Execute(ctx context.Context) error {
	return nil
}

func (p *LogsPipeline) ExecuteSync(ctx context.Context) []error {
	return executeSync[model.Log](
		ctx,
		p.receivers,
		p.processorList,
		p.exporters,
	)
}

func (p *LogsPipeline) Start(ctx context.Context, wg *sync.WaitGroup) error {
	return start[model.Log](
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


func (p *LogsPipeline) Shutdown(ctx context.Context) error {
	return shutdown[model.Log](
		ctx,
		p.InputChan,
		p.receiverChans,
		p.exportChan,
		p.resultChan,
	)
}
