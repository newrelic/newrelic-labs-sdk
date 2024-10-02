package pipeline

import "context"

type (
	ExporterFunc[T interface{}] func(context.Context, []T) error
)

type Exporter[T interface{}] interface {
	GetId() string
	Export(context.Context, []T) error
}

type ExporterAdapter[T interface{}] struct {
	id			string
	exporter	ExporterFunc[T]
}

func NewExporterAdapter[T interface{}](
	id string,
	exporter ExporterFunc[T],
) *ExporterAdapter[T] {
	return &ExporterAdapter[T] { id, exporter }
}

func (r *ExporterAdapter[T]) GetId() string {
	return r.id
}

func (r *ExporterAdapter[T]) Export(ctx context.Context, data []T) error {
	return r.exporter(ctx, data)
}
