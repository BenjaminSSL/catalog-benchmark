package common

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
)

type RequestBuilder struct {
	host     string
	path     string
	method   string
	endpoint string
	body     []byte
	query    url.Values
	headers  http.Header
}

func NewRequestBuilder() *RequestBuilder {

	builder := &RequestBuilder{
		method:  "GET",
		headers: make(http.Header),
		query:   make(url.Values),
	}

	return builder
}

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
	b.query.Add(key, value)
	return b
}

func (b *RequestBuilder) SetJSONBody(body []byte) *RequestBuilder {
	b.body = body
	return b.AddHeader("Content-Type", "application/json")
}

func (b *RequestBuilder) Build(ctx context.Context, host string, resource string, token string) (*http.Request, error) {
	baseURL := fmt.Sprintf("http://%s%s", host, path.Clean(resource))
	endpoint := path.Join("/", b.endpoint)

	fullURL := fmt.Sprintf("%s%s", baseURL, endpoint)

	if len(b.query) > 0 {
		fullURL = fmt.Sprintf("%s?%s", fullURL, b.query.Encode())
	}

	req, err := http.NewRequestWithContext(ctx, b.method, fullURL, bytes.NewBuffer(b.body))
	if err != nil {
		return nil, err
	}

	req.Header = b.headers

	if token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	return req, nil
}
