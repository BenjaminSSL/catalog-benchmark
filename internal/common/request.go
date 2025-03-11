package common

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
)

type RequestParams interface {
	Validate() error
}
type RequestConfig struct {
	Host  string
	Token string
	Path  string // The default API path to be used when no specific endpoint is provided
}

func GetConfig(ctx context.Context) RequestConfig {
	if config, ok := ctx.Value("config").(RequestConfig); ok {
		return config
	}
	return RequestConfig{}
}

func GetRequestConfigFromEnv(catalogName string) (RequestConfig, error) {
	var host string
	var token string
	var defaultPath string
	var err error

	// Set up the catalogName and their factory
	if catalogName == "polaris" {
		host = os.Getenv("POLARIS_HOST")
		defaultPath = os.Getenv("POLARIS_PATH")

		token, err = FetchPolarisToken(host)
		if err != nil {
			return RequestConfig{}, err
		}

		log.Printf("Fetched the token from Polaris")

	} else if catalogName == "unity" {
		host = os.Getenv("UNITY_HOST")
		defaultPath = os.Getenv("UNITY_PATH")
	}

	return RequestConfig{host, token, defaultPath}, nil
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

func (b *RequestBuilder) Build(ctx context.Context) (*http.Request, error) {
	config := GetConfig(ctx)
	baseURL := fmt.Sprintf("http://%s%s", config.Host, path.Clean(config.Path))
	endpoint := path.Join("/", b.endpoint)

	url := fmt.Sprintf("%s%s", baseURL, endpoint)

	if len(b.query) > 0 {
		url = url + "?" + b.buildQuery()
	}

	req, err := http.NewRequestWithContext(ctx, b.method, url, bytes.NewBuffer(b.body))
	if err != nil {
		return nil, err
	}

	req.Header = b.headers

	if config.Token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", config.Token))
	}
	return req, nil

}
