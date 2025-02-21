package common

import (
	"bytes"
	"fmt"
	"net/http"
	"path"
)

type RequestContext struct {
	Host  string
	Token string
}

type RequestBuilder struct {
	context  RequestContext
	method   string
	endpoint string
	body     []byte
	headers  http.Header
}

func NewRequestBuilder(context RequestContext) *RequestBuilder {
	return &RequestBuilder{
		context: context,
		headers: make(http.Header),
	}
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

func (b *RequestBuilder) SetJSONBody(body []byte) *RequestBuilder {
	b.body = body
	return b.AddHeader("Content-Type", "application/json")
}

func (b *RequestBuilder) Build() (*http.Request, error) {
	url := fmt.Sprintf("http://%s%s", b.context.Host, path.Join("/", b.endpoint))

	req, err := http.NewRequest(b.method, url, bytes.NewBuffer(b.body))
	if err != nil {
		return nil, err
	}

	req.Header = b.headers

	// Automatically set the authorization token if it is set
	if b.context.Token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", b.context.Token))
	}

	return req, nil

}
