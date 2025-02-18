package execution

import (
	"bytes"
	"log"
	"net/http"
)

type Request struct {
	URL     string
	Method  string
	Body    []byte
	Headers http.Header
}

func (request Request) Execute() int {

	req, err := http.NewRequest(request.Method, request.URL, bytes.NewBuffer(request.Body))
	if err != nil {
		panic(err)
	}

	req.Header = request.Headers

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Error executing request: %s", err)
		return 0
	}

	return resp.StatusCode

}
