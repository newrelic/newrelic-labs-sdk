package connectors

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
)

const (
	defaultTimeout = 5 * time.Second
)

type HttpConnector struct {
	Method        		string
	Url           		string
	UserAgent			string
	Headers       		map[string]string
	Body          		any
	Authenticator 		HttpAuthenticator
	Timeout			    time.Duration
}

type HttpAuthenticator interface {
	Authenticate(connector *HttpConnector, req *http.Request) error
}

type HttpBodyBuilder func () (any, error)

// Build and Connector for HTTP GET requests.
func NewHttpGetConnector(url string) *HttpConnector {
	return &HttpConnector{
		Method:  "GET",
		Url:     url,
		Timeout: defaultTimeout,
	}
}

// Build and Connector for HTTP POST requests.
func NewHttpPostConnector(
	url string,
	body any,
) *HttpConnector {
	return &HttpConnector{
		Method:  "POST",
		Url:     url,
		Body:    body,
		Timeout: defaultTimeout,
	}
}

func (c *HttpConnector) Request() (io.ReadCloser, error) {
	switch strings.ToLower(c.Method) {
	case "get":
		return c.httpGet()
		//return []byte(res), err

	case "post":
		reader, err := buildPostBody(c.Body)
		if err != nil {
			return nil, err
		}

		return c.httpPost(reader)

	default:
		return nil, fmt.Errorf("unsupported HTTP method")
	}
}

func (c *HttpConnector) RequestBytes() ([]byte, error) {
	readCloser, err := c.Request()
	if err !=  nil {
		return nil, err
	}

	defer readCloser.Close()

	return io.ReadAll(readCloser)
}

func (c *HttpConnector) RequestString() (string, error) {
	bytes, err := c.RequestBytes()
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func (c *HttpConnector) SetAuthenticator(authenticator HttpAuthenticator) {
	c.Authenticator = authenticator
}

func (c *HttpConnector) SetMethod(method string) {
	c.Method = method
}

func (c *HttpConnector) SetUrl(url string) {
	c.Url = url
}

func (c *HttpConnector) SetBody(body any) {
	c.Body = body
}

func (c *HttpConnector) SetUserAgent(userAgent string) {
	c.UserAgent = userAgent
}

func (c *HttpConnector) SetHeaders(headers map[string]string) {
	c.Headers = headers
}

func (c *HttpConnector) SetTimeout(timeout time.Duration) {
	c.Timeout = timeout
}

func (c *HttpConnector) httpGet() (io.ReadCloser, error) {
	log.Debugf("creating HTTP GET request for %s", c.Url)

	req, err := http.NewRequest("GET", c.Url, nil)
	if err != nil {
		return nil, err
	}

	for key, val := range c.Headers {
		req.Header.Add(key, val)
	}

	userAgent := c.UserAgent
	if userAgent == "" {
		userAgent = GetUserAgent()
	}

	req.Header.Add("User-Agent", userAgent)

	if c.Authenticator != nil {
		log.Debugf("authenticating request for %s", c.Url)
		err = c.Authenticator.Authenticate(c, req)
		if err != nil {
			return nil, err
		}
	}

	client := http.DefaultClient
	client.Timeout = c.Timeout

	log.Debugf("performing request for %s", c.Url)
	resp, err := client.Do(req)

	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf(
				"error connecting to %s with status %d: %w",
				c.Url,
				resp.StatusCode,
				err,
			)
		}

		return nil, fmt.Errorf(
			"error connecting to %s: %w",
			c.Url,
			err,
		)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf(
			"error connecting to %s with status %d",
			c.Url,
			resp.StatusCode,
		)
	}

	return resp.Body, nil
}

func (c *HttpConnector) httpPost(reqBody io.Reader) (io.ReadCloser, error) {
	log.Debugf("creating HTTP POST request for %s", c.Url)

	req, err := http.NewRequest("POST", c.Url, reqBody)
	if err != nil {
		return nil, err
	}

	for key, val := range c.Headers {
		req.Header.Add(key, val)
	}

	userAgent := c.UserAgent
	if userAgent == "" {
		userAgent = GetUserAgent()
	}

	req.Header.Add("User-Agent", userAgent)

	if c.Authenticator != nil {
		log.Debugf("authenticating request for %s", c.Url)
		err = c.Authenticator.Authenticate(c, req)
		if err != nil {
			return nil, err
		}
	}

	client := http.DefaultClient
	client.Timeout = c.Timeout

	log.Debugf("performing request for %s", c.Url)
	resp, err := client.Do(req)

	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf(
				"error connecting to %s with status %d: %w",
				c.Url,
				resp.StatusCode,
				err,
			)
		}

		return nil, fmt.Errorf(
			"error connecting to %s: %w",
			c.Url,
			err,
		)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf(
			"error connecting to %s with status %d",
			c.Url,
			resp.StatusCode,
		)
	}

	return resp.Body, nil
}

func buildPostBody(b any) (io.Reader, error) {
	switch body := b.(type) {
	case io.Reader:
		return body, nil
	case string:
		return strings.NewReader(body), nil
	case []byte:
		return bytes.NewReader(body), nil
	case HttpBodyBuilder:
		c, err := body()
		if err != nil {
			return nil, err
		}
		return buildPostBody(c)

	default:
		return nil, fmt.Errorf("unsupported type for body")
	}
}

func GetUserAgent() string {
	return fmt.Sprintf(
		"newrelic-labs-sdk (%s; %s)",
		runtime.GOOS,
		runtime.GOARCH,
	)
}
