package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/log"
	"github.com/spf13/viper"
)

const (
	DEFAULT_HARVEST_TIME = 60
	DEFAULT_ITEMS_PER_BATCH = 500
	DEFAULT_MAX_EXPORT_WORKERS = 2
)

var gPollSignal = struct{}{}

type Pipeline interface {
	Start(ctx context.Context, wg *sync.WaitGroup) error
	Execute(ctx context.Context) error
	ExecuteSync(ctx context.Context) []error
	Shutdown(ctx context.Context) error
}

type Component interface {
	GetId() string
}

type pipeline[T interface{}] struct {
	InputChan 		chan T
	receivers 		[]Receiver[T]
	receiverChans	[]chan struct{}
	processorList 	*ProcessorList[T]
	exporters		[]Exporter[T]
	exportChan		chan []T
	resultChan		chan error
}

func execute(
	ctx context.Context,
	receiverChans []chan struct{},
) error {
	for i := 0; i < len(receiverChans); i += 1 {
		receiverChans[i] <- gPollSignal
	}

	return nil
}

func executeSync[T interface{}](
	ctx context.Context,
	receivers []Receiver[T],
	processorList *ProcessorList[T],
	exporters []Exporter[T],
) []error {
	var wg sync.WaitGroup

	log.Debugf("executing synchronous pipeline")
	defer log.Debugf("synchronous pipeline execution complete")

	errs := []error{}
	dataChan := make(chan T)
	exportChan := make(chan []T)
	resultChan := make(chan error)

	wg.Add(1)
	log.Debugf("starting processor worker")
	go processorWorker[T](
		ctx,
		&wg,
		processorList,
		dataChan,
		exportChan,
		resultChan,
		true,
	)

	wg.Add(1)
	log.Debugf("starting exporter worker")
	go exporterWorker[T](ctx, &wg, exporters, exportChan, resultChan)

	for i := 0; i < len(receivers); i += 1 {
		log.Debugf("running receiver %s", receivers[i].GetId())

		err := receivers[i].Poll(ctx, dataChan)
		if err != nil {
			errs = append(errs, err)
			break
		}
	}

	log.Debugf("starting result reader")
	go func() {
		done := false
		for ; !done; {
			err, ok := <- resultChan
			if !ok {
				log.Debugf("result channel closed")
				done = true
				continue
			}

			errs = append(errs, err)
		}
	}()

	close(dataChan)
	wg.Wait()

	close(resultChan)

	if (len(errs) > 0) {
		log.Debugf("completed with errors: %v", errors.Join(errs...))
	}

	return errs
}

func start[T interface{}](
	ctx context.Context,
	wg *sync.WaitGroup,
	dataChan chan T,
	receivers []Receiver[T],
	receiverChans []chan struct{},
	processorList *ProcessorList[T],
	exporters []Exporter[T],
	exportChan chan []T,
	resultChan chan error,
) error {
	for i := 0; i < len(receivers); i += 1 {
		receiverChans = append(receiverChans, make(chan struct{}))

		wg.Add(1)
		go receiverWorker(
			ctx,
			wg,
			receivers[i],
			receiverChans[i],
			dataChan,
			resultChan,
		)
	}

	maxExportWorkers := viper.GetInt("pipeline.exportWorkers")
	if maxExportWorkers == 0 {
		maxExportWorkers = DEFAULT_MAX_EXPORT_WORKERS
	}

	log.Debugf("starting %d export workers", maxExportWorkers)

	for i := 0; i < maxExportWorkers; i += 1 {
		wg.Add(1)
		go exporterWorker(ctx, wg, exporters, exportChan, resultChan)
	}

	// @TODO: right now this is a single reader worker and is a bottleneck
	wg.Add(1)
	go processorWorker(
		ctx,
		wg,
		processorList,
		dataChan,
		exportChan,
		resultChan,
		false,
	)

	wg.Add(1)
	go resultWorker(ctx, wg, resultChan)

	return nil
}

func shutdown[T interface{}](
	ctx context.Context,
	dataChan chan T,
	receiverChans []chan struct{},
	exportChan chan []T,
	resultChan chan error,
) error {
	close(dataChan)

	for i := 0; i < len(receiverChans); i += 1 {
		close(receiverChans[i])
	}

	close(exportChan)
	close(resultChan)

	return nil
}

func receiverWorker[T interface{}](
	ctx context.Context,
	wg *sync.WaitGroup,
	poller Receiver[T],
	pollChan chan struct{},
	dataChan chan T,
	resultChan chan error,
) {
	defer wg.Done()

	for {
		select {
		case _, ok := <- pollChan:
			if !ok {
				resultChan <- nil
				return
			}

			log.Debugf("running poll for receiver %s", poller.GetId())

			err := poller.Poll(ctx, dataChan)
			if err != nil {
				log.Errorf("poll failed: %v", err)
			}

			resultChan <- err

		case <- ctx.Done():
			resultChan <- nil
			return
		}
	}
}

func flush[T any](
	processorList *ProcessorList[T],
	telemetry []T,
	exportChan chan []T,
) error {
	log.Debugf("processor flushing %d items", len(telemetry))

	newMetrics, err := processorList.Process(telemetry)
	if err != nil {
		return err
	}

	exportChan <- newMetrics

	return nil
}

func processorWorker[T any](
	ctx context.Context,
	wg *sync.WaitGroup,
	processorList *ProcessorList[T],
	dataChan chan T,
	exportChan chan []T,
	resultChan chan error,
	closeOnCompletion bool,
) {
	defer wg.Done()

	itemsPerBatch := viper.GetInt("pipeline.itemsPerBatch")
	if itemsPerBatch == 0 {
		itemsPerBatch = DEFAULT_ITEMS_PER_BATCH
	}

	log.Debugf("using %d items per batch", itemsPerBatch)
	data := make([]T, 0, itemsPerBatch)

	harvestTime := viper.GetInt("pipeline.harvestTime")
	if harvestTime == 0 {
		harvestTime = DEFAULT_HARVEST_TIME
	}

	log.Debugf("using %d harvest time", harvestTime)
	ticker := time.NewTicker(time.Duration(harvestTime) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case datum, ok := <- dataChan:
			if !ok {
				log.Debugf("data channel closed")

				err := flush(processorList, data, exportChan)
				if err != nil {
					log.Errorf("flush failed: %v", err)
					resultChan <- err
				}

				if closeOnCompletion {
					log.Debugf("closing export channel")
					close(exportChan)
				}
				return
			}

			data = append(data, datum)

			if len(data) + 20 >= itemsPerBatch {
				err := flush(processorList, data, exportChan)
				if err != nil {
					// @TODO: should we return here or keep processing more
					// metrics? Should we keep the batches we couldn't send
					// somewhere and try to resend them later?
					// That sounds like work.
					log.Errorf("flush failed: %v", err)
					resultChan <- err
				}
				data = make([]T, 0, itemsPerBatch)
			}

		case <-ticker.C:
			log.Debugf("harvest timer ticked; flushing items")

			err := flush(processorList, data, exportChan)
			if err != nil {
				// @TODO: should we return here or keep processing more
				// metrics? Should we keep the batches we couldn't send
				// somewhere and try to resend them later?
				// That sounds like work.
				log.Errorf("flush failed: %v", err)
				resultChan <- err
			}
			data = make([]T, 0, itemsPerBatch)

		case <- ctx.Done():
			return
		}
	}
}

func exporterWorker[T any](
	ctx context.Context,
	wg *sync.WaitGroup,
	exporters []Exporter[T],
	exportChan chan []T,
	resultChan chan error,
) {
	defer wg.Done()

	for {
		select {
		case data, ok := <- exportChan:
			if !ok {
				log.Debugf("exporter channel closed")
				return
			}

			for i := 0; i < len(exporters); i += 1 {
				log.Debugf("running export for receiver %s", exporters[i].GetId())

				err := exporters[i].Export(ctx, data)
				if err != nil {
					log.Errorf("exporter failed: %v", err)
					resultChan <- err
				}
			}

		case <- ctx.Done():
			return
		}
	}
}

func resultWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	resultChan chan error,
) {
	defer wg.Done()

	for {
		select {
		case err, ok := <- resultChan:
			if !ok {
				return
			}

			if err != nil {
				log.Errorf("worker error: %v", err)
			}

		case <- ctx.Done():
			return
		}
	}
}
