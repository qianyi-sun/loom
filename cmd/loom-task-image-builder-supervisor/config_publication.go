package main

import (
	"encoding/json"
	"errors"
	"strings"
)

// Public release trust only. Job/session-bound credentials come from the guard,
// never from configuration, environment, or ambient Docker credentials.
type publicationDiskConfig struct {
	Origin     string               `json:"origin"`
	Service    string               `json:"service"`
	ServerName string               `json:"server_name"`
	Issuer     string               `json:"issuer"`
	KeyID      string               `json:"key_id"`
	CA         executableDiskConfig `json:"ca"`
}

func validatePublicationConfigFields(payload []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return errors.New("publication configuration invalid")
	}
	for key, value := range fields {
		if !strings.EqualFold(key, "publication") {
			continue
		}
		if key != "publication" || requireRegisteredJSONFields(value, "origin", "service", "server_name", "issuer", "key_id", "ca") != nil {
			return errors.New("publication configuration fields invalid")
		}
		var publication map[string]json.RawMessage
		if json.Unmarshal(value, &publication) != nil || requireRegisteredJSONFields(publication["ca"], "path", "sha256") != nil {
			return errors.New("publication CA configuration fields invalid")
		}
	}
	return nil
}

func loadPublicationHandoff(disk publicationDiskConfig, releaseRoot string) (*RegistryPublicationHandoff, error) {
	payload, err := loadReleaseCAPEM(disk.CA, releaseRoot)
	if err != nil {
		return nil, err
	}
	policy, err := NewRegistryUploadPolicy(disk.Origin, disk.Service, payload, disk.ServerName)
	if err != nil {
		return nil, err
	}
	return NewRegistryPublicationHandoff(policy, disk.Issuer, disk.KeyID)
}
