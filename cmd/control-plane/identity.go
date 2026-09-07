package main

import (
	"errors"
	"os"
	"strings"

	"github.com/pazhou/supaflow/internal/api"
)

func configureAuthentication() (*api.Authenticator, error) {
	if strings.TrimSpace(os.Getenv("SUPAFLOW_API_KEYS_JSON")) != "" ||
		strings.TrimSpace(os.Getenv("SUPAFLOW_BOOTSTRAP_API_KEYS_JSON")) != "" ||
		enabled(os.Getenv("SUPAFLOW_IDENTITY_BOOTSTRAP_ENABLED")) {
		return nil, errors.New("legacy API key and identity bootstrap configuration is no longer accepted")
	}
	current, err := readSecretFile("SUPAFLOW_MASTER_KEY_FILE", true)
	if err != nil {
		return nil, err
	}
	previous, err := readSecretFile("SUPAFLOW_PREVIOUS_MASTER_KEY_FILE", false)
	if err != nil {
		return nil, err
	}
	return api.NewMasterKeyAuthenticator(current, previous)
}

func readSecretFile(environmentName string, required bool) (string, error) {
	path := strings.TrimSpace(os.Getenv(environmentName))
	if path == "" {
		if required {
			return "", errors.New(environmentName + " is required")
		}
		return "", nil
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", errors.New(environmentName + " cannot be read")
	}
	value := strings.TrimSpace(string(contents))
	if len([]byte(value)) < 32 {
		return "", errors.New(environmentName + " must contain at least 32 bytes")
	}
	return value, nil
}

func enabled(value string) bool { return strings.EqualFold(strings.TrimSpace(value), "true") }
