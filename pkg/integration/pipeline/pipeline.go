package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/spf13/viper"
)

const (
	DEFAULT_HARVEST_INTERVAL = 60
	DEFAULT_RECEIVE_BUFFER_SIZE = 500
	DEFAULT_PIPELINE_INSTANCES = 3
)

var gPollSignal = struct{}{}

type PipelineInstance[T interface{}] struct {
	pollChan	chan struct{}
	receivers 	[]Receiver[T]
	dataChan  	chan T
	processors	*ProcessorList[T]
	exportChan  chan []T
	exporters 	[]Exporter[T]
	resultChan  chan error
}

type pipeline[T interface{}] struct {
	id				string
	receivers 		[]Receiver[T]
	processorList 	*ProcessorList[T]
	exporters		[]Exporter[T]
	instances		[]PipelineInstance[T]
}

func execute[T interface{}](
	_ context.Context,
	instances []PipelineInstance[T],
) error {
	for i := 0; i < len(instances); i += 1 {
		log.Debugf("sending poll signal to pipeline instance %d", i)
		instances[i].pollChan <- gPollSignal
	}

	return nil
}

func executeSync[T interface{}](
	ctx context.Context,
	receivers []Receiver[T],
	processorList *ProcessorList[T],
	exporters []Exporter[T],
) error {
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
		//true,
	)

	wg.Add(1)
	log.Debugf("starting exporter worker")
	go exporterWorker[T](ctx, &wg, exporters, exportChan, resultChan)

	log.Debugf("starting result worker")
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

	for i := 0; i < len(receivers); i += 1 {
		log.Debugf("running receiver %s", receivers[i].GetId())

		err := receivers[i].Poll(ctx, dataChan)
		if err != nil {
			errs = append(errs, err)
			break
		}
	}

	close(dataChan)
	wg.Wait()

	if (len(errs) > 0) {
		err := errors.Join(errs...)

		log.Debugf("completed with errors: %v", err)

		return err
	}

	return nil
}

func start[T interface{}](
	ctx context.Context,
	wg *sync.WaitGroup,
	receivers []Receiver[T],
	processorList *ProcessorList[T],
	exporters []Exporter[T],
) ([]PipelineInstance[T], error) {
	instances := []PipelineInstance[T]{}

	maxInstances := viper.GetInt("pipeline.instances")
	if maxInstances == 0 {
		log.Debugf(
			"defaulting pipeline instances to %d", DEFAULT_PIPELINE_INSTANCES,
		)
		maxInstances = DEFAULT_PIPELINE_INSTANCES
	}

	if len(receivers) < maxInstances {
		maxInstances = len(receivers)
	}

	receiverGroups := make([][]Receiver[T], maxInstances)

	group := 0
	for i := 0; i < len(receivers); i += 1 {
		receiverGroups[group] = append(receiverGroups[group], receivers[i])
		group += 1
		if group == maxInstances {
			group = 0
		}
	}

	for i := 0; i < maxInstances; i += 1 {
		instance := PipelineInstance[T]{
			make(chan struct{}),
			receiverGroups[i],
			make(chan T),
			processorList,
			make(chan []T),
			exporters,
			make(chan error),
		}

		log.Debugf("starting receiver worker %d", i)

		wg.Add(1)
		go receiverWorker(
			ctx,
			wg,
			instance.receivers,
			instance.pollChan,
			instance.dataChan,
			instance.resultChan,
		)

		log.Debugf("starting processor worker %d", i)

		wg.Add(1)
		go processorWorker(
			ctx,
			wg,
			processorList,
			instance.dataChan,
			instance.exportChan,
			instance.resultChan,
			//false,
		)

		log.Debugf("starting exporter worker %d", i)

		wg.Add(1)
		go exporterWorker(
			ctx,
			wg,
			instance.exporters,
			instance.exportChan,
			instance.resultChan,
		)

		log.Debugf("starting result worker %d", i)

		wg.Add(1)
		go resultWorker(ctx, wg, instance.resultChan)

		instances = append(instances, instance)
	}

	return instances, nil
}

func shutdown[T interface{}](
	_ context.Context,
	instances []PipelineInstance[T],
) error {
	for _, instance := range instances {
		// Closing the poll channel will shutdown the receiver worker which will
		// then shutdown the data channel which will shutdown the processor
		// worker which will close the export channel, then the exporter worker
		// will close the result channel and shutdown the export worker
		close(instance.pollChan)
	}

	return nil
}

func receiverWorker[T interface{}](
	ctx context.Context,
	wg *sync.WaitGroup,
	receivers []Receiver[T],
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
				close(dataChan)
				return
			}

			for _, receiver := range receivers {
				log.Debugf("running poll for receiver %s", receiver.GetId())

				err := receiver.Poll(ctx, dataChan)
				if err != nil {
					log.Errorf("poll failed: %v", err)
				}

				resultChan <- err
			}

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
	//closeOnCompletion bool,
) {
	defer wg.Done()

	receiveBufferSize := viper.GetInt("pipeline.receiveBufferSize")
	if receiveBufferSize == 0 {
		receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE
	}

	log.Debugf("using %d receive buffer size", receiveBufferSize)
	data := make([]T, 0, receiveBufferSize)

	harvestInterval := viper.GetInt("pipeline.harvestInterval")
	if harvestInterval == 0 {
		harvestInterval = DEFAULT_HARVEST_INTERVAL
	}

	log.Debugf("using %d harvest interval", harvestInterval)
	ticker := time.NewTicker(time.Duration(harvestInterval) * time.Second)
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

				// @todo: Not sure what the original purpose of this flag was
				// but I do not think it is needed. Once the processor is done,
				// nothing more will be sent to the exporter so closing the
				// channel will signal nothing more is coming and the exporter
				// can shutdown.
				//if closeOnCompletion {
					log.Debugf("closing export channel")
					close(exportChan)
				//}
				return
			}

			data = append(data, datum)

			if len(data) + 20 >= receiveBufferSize {
				err := flush(processorList, data, exportChan)
				if err != nil {
					// @TODO: should we return here or keep processing more
					// metrics? Should we keep the batches we couldn't send
					// somewhere and try to resend them later?
					// That sounds like work.
					log.Errorf("flush failed: %v", err)
					resultChan <- err
				}
				data = make([]T, 0, receiveBufferSize)
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
			data = make([]T, 0, receiveBufferSize)

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
				// If the exporter channel is closed, it means the receiver
				// and processor have shutdown so they won't send anything else
				// on the result channel so shut it down. This will stop the
				// result worker and release the wait group lock.
				close(resultChan)
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
