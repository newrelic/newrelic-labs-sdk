package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/connectors"
	"github.com/newrelic/newrelic-labs-sdk/pkg/integration/model"
)

type (
	simpleReceiverOpt func(receiver *SimpleReceiver)
	ReceiverFunc[T interface{}] func(context.Context, chan <- T) error
)

type Receiver[T interface{}] interface {
	Component
	Poll(context.Context, chan <- T) error
}

type ReceiverAdapter[T interface{}] struct {
	id			string
	poller		ReceiverFunc[T]
}

func NewReceiverAdapter[T interface{}](
	id string,
	poller ReceiverFunc[T],
) *ReceiverAdapter[T] {
	return &ReceiverAdapter[T] { id, poller }
}

func (r *ReceiverAdapter[T]) GetId() string {
	return r.id
}

func (r *ReceiverAdapter[T]) Poll(ctx context.Context, ch chan <- T) error {
	return r.poller(ctx, ch)
}

type SimpleReceiver struct {
	id				string
	connector    	*connectors.HttpConnector
	metricsDecoder 	MetricsDecoderFunc
	eventsDecoder	EventsDecoderFunc
	logsDecoder		LogsDecoderFunc
}

func NewSimpleReceiver(
	id string,
	url string,
	simpleReceiverOpts ...simpleReceiverOpt,
) *SimpleReceiver {
	receiver := &SimpleReceiver{
		id: id,
		connector: connectors.NewHttpGetConnector(url),
	}

	for _, opt := range simpleReceiverOpts {
		opt(receiver)
	}

	return receiver
}

func (r *SimpleReceiver) GetId() string {
	return r.id
}

func WithAuthenticator(
	authenticator connectors.HttpAuthenticator,
) simpleReceiverOpt {
	return func(r *SimpleReceiver) {
		r.connector.SetAuthenticator(authenticator)
	}
}

func WithMethod(method string) simpleReceiverOpt {
	return func(r *SimpleReceiver) {
		r.connector.SetMethod(method)
	}
}

func WithBody(body any) simpleReceiverOpt {
	return func(r *SimpleReceiver) {
		r.connector.SetBody(body)
	}
}

func WithHeaders(headers map[string]string) simpleReceiverOpt {
	return func(r *SimpleReceiver) {
		r.connector.SetHeaders(headers)
	}
}

func WithTimeout(timeout time.Duration) simpleReceiverOpt {
	return func(r *SimpleReceiver) {
		r.connector.SetTimeout(timeout)
	}
}

func WithMetricsDecoder(decoder MetricsDecoderFunc) simpleReceiverOpt {
	return func(r *SimpleReceiver) {
		r.metricsDecoder = decoder
	}
}

func WithEventsDecoder(decoder EventsDecoderFunc) simpleReceiverOpt {
	return func(r *SimpleReceiver) {
		r.eventsDecoder = decoder
	}
}

func WithLogsDecoder(decoder LogsDecoderFunc) simpleReceiverOpt {
	return func(r *SimpleReceiver) {
		r.logsDecoder = decoder
	}
}

func (s *SimpleReceiver) PollMetrics(
	ctx context.Context,
	metricChan chan <- model.Metric,
) error {
	data, err := s.connector.Request()
	if err != nil {
		fmt.Printf("%v", err)
		return err
	}

	err = s.metricsDecoder(s, data, metricChan)
	if err != nil {
		return err
	}

	return nil
}


func (s *SimpleReceiver) PollEvents(
	ctx context.Context,
	out chan <- model.Event,
) error {
	data, err := s.connector.Request()
	if err != nil {
		return err
	}

	err = s.eventsDecoder(s, data, out)
	if err != nil {
		return err
	}

	return nil
}

func (s *SimpleReceiver) PollLogs(
	ctx context.Context,
	out chan <- model.Log,
) error {
	data, err := s.connector.Request()
	if err != nil {
		return err
	}

	err = s.logsDecoder(s, data, out)
	if err != nil {
		return err
	}

	return nil
}

/*
type DataSource interface {
	Fetch() (interface{}, error)
}

type GenericReceiver struct {
	dataSource  DataSource
	transformer TransformerFunc
}

func NewGenericReceiver(
	dataSource DataSource,
	transformer TransformerFunc,
) *GenericReceiver {
	return &GenericReceiver{
		dataSource,
		transformer,
	}
}

func (r *GenericReceiver) Poll(
	ctx context.Context,
	sink model.MeltSink,
) error {
	model, err := r.dataSource.Fetch()
	if err != nil {
		return err
	}

	return r.transformer(model, sink)
}
*/
