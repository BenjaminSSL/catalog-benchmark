package common

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"path"
)

type RequestParams interface {
	Validate() error
}

type RequestBuilder struct {
	host     string
	path     string
	method   string
	endpoint string
	body     []byte
	query    map[string]string
	headers  http.Header
}

func NewRequestBuilder() *RequestBuilder {

	builder := &RequestBuilder{
		method:  "GET",
		headers: make(http.Header),
		query:   make(map[string]string),
	}

	return builder
}

// SetMethod sets the requests method. The default method is GET.
func (b *RequestBuilder) SetMethod(method string) *RequestBuilder {
	b.method = method
	return b
}
func (b *RequestBuilder) SetEndpoint(endpoint string) *RequestBuilder {
	b.endpoint = endpoint
	return b
}

func (b *RequestBuilder) AddHeader(key string, value string) *RequestBuilder {
	b.headers.Add(key, value)
	return b
}

func (b *RequestBuilder) AddQueryParam(key string, value string) *RequestBuilder {
	b.query[key] = value
	return b
}

func (b *RequestBuilder) SetJSONBody(body []byte) *RequestBuilder {
	b.body = body
	return b.AddHeader("Content-Type", "application/json")
}

func (b *RequestBuilder) buildQuery() string {
	query := ""
	for key, value := range b.query {
		if query == "" {
			query = fmt.Sprintf("%s=%s", key, value)
		} else {
			query = fmt.Sprintf("%s&%s=%s", query, key, value)
		}
	}
	return query
}

func (b *RequestBuilder) Build(ctx context.Context, host string, resource string, token string) *http.Request {
	baseURL := fmt.Sprintf("http://%s%s", host, path.Clean(resource))
	endpoint := path.Join("/", b.endpoint)

	url := fmt.Sprintf("%s%s", baseURL, endpoint)

	if len(b.query) > 0 {
		url = url + "?" + b.buildQuery()
	}

	req, _ := http.NewRequestWithContext(ctx, b.method, url, bytes.NewBuffer(b.body))

	req.Header = b.headers

	if token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	}
	return req

}
