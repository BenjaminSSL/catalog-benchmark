package common

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
)

func FetchPolarisToken(host string) (string, error) {
	id := os.Getenv("POLARIS_CLIENT_ID")
	secret := os.Getenv("POLARIS_CLIENT_SECRET")

	if id == "" || secret == "" {
		return "", fmt.Errorf("client-id and client-secret variables must be set")
	}

	oauthURL := fmt.Sprintf("http://%s/api/catalog/v1/oauth/tokens", host)

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", id)
	form.Set("client_secret", secret)
	form.Set("scope", "PRINCIPAL_ROLE:ALL")

	req, err := http.NewRequest("POST", oauthURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var response struct {
		AccessToken string `json:"access_token"`
	}

	if err := decoder.Decode(&response); err != nil {
		return "", err
	}

	return response.AccessToken, nil

}
