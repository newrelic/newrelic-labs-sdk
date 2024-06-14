package connectors

import "io"

type Connector interface {
	Request() (io.ReadCloser, error)
}
