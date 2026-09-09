package main

import (
	"context"
	"encoding/json"
	"errors"
	"syscall"
)

func (c *GuardClient) PublicationSubmit(ctx context.Context, binding publicationStatusBinding, current *SecretBuffer) (*publicationStatus, error) {
	return c.publicationOperation(ctx, "publication-submit", binding, current)
}

func (c *GuardClient) PublicationPoll(ctx context.Context, binding publicationStatusBinding, current *SecretBuffer) (*publicationStatus, error) {
	return c.publicationOperation(ctx, "publication-poll", binding, current)
}

func (c *GuardClient) publicationOperation(ctx context.Context, operation string, binding publicationStatusBinding, current *SecretBuffer) (*publicationStatus, error) {
	invalid := errors.New("publication response invalid")
	if ctx == nil || !validPublicationStatusBinding(binding) || (operation != "publication-submit" && operation != "publication-poll") {
		return nil, errors.New("publication request invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Complete-set and snapshot pins are locally derived expectations. They are
	// never caller authority sent to the guard or used to choose registry inputs.
	request := map[string]any{
		"schema": localSchema, "operation": operation,
		"grant_id": binding.GrantID, "operation_id": binding.OperationID,
		"materialization_id": binding.MaterializationID, "attempt_id": binding.AttemptID,
		"lease_epoch": binding.LeaseEpoch,
	}
	fd, err := current.cloneSealedMemfd("session-publication", maxSecretBytes)
	if err != nil {
		return nil, err
	}
	packet, rights, err := c.roundTrip(ctx, request, []int{fd})
	syscall.Close(fd)
	if err != nil {
		return nil, err
	}
	defer closeRights(rights)
	defer packet.Close()
	if len(rights) != 0 {
		return nil, invalid
	}
	fields, err := scanJSONObjectFields(packet.payload)
	if err != nil || len(fields) != 5 {
		return nil, invalid
	}
	for _, field := range []string{"schema", "operation", "response_id", "grant_id", "publication_status"} {
		if _, ok := fields[field]; !ok {
			return nil, invalid
		}
	}
	var response struct {
		Schema     string          `json:"schema"`
		Operation  string          `json:"operation"`
		ResponseID string          `json:"response_id"`
		GrantID    string          `json:"grant_id"`
		Status     json.RawMessage `json:"publication_status"`
	}
	if err := decodeStrictJSON(packet.payload, &response); err != nil || response.Schema != localSchema ||
		response.Operation != operation || response.GrantID != binding.GrantID || !isCanonicalNonZeroUUID(response.ResponseID) {
		return nil, invalid
	}
	status, err := parsePublicationStatus(response.Status, binding)
	if err != nil {
		return nil, invalid
	}
	if err := c.ackPacket(packet, response.ResponseID); err != nil {
		return nil, err
	}
	return &status, nil
}
