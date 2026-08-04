package endpoint

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"

	"github.com/pkg/errors"

	"layered/transactioned/service"
)

type updateRideRequest struct {
	RideID string `json:"ride_id"`
}

var rideIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

func (r updateRideRequest) validate() error {
	if r.RideID == "" {
		return errors.New("ride_id is required")
	}
	if !rideIDPattern.MatchString(r.RideID) {
		return errors.New("ride_id has invalid format")
	}
	return nil
}

func errPayload(msg string) string {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return string(b)
}

type Handler struct {
	svc *service.Service
}

func New(svc *service.Service) *Handler {
	return &Handler{svc: svc}
}

// UpdateRide parses reqBody, validates, calls the service, and maps the result to
// (statusCode, payload) based on error type.
func (h *Handler) UpdateRide(ctx context.Context, reqBody string) (int, string) {
	var req updateRideRequest
	err := json.Unmarshal([]byte(reqBody), &req)
	if err != nil {
		return http.StatusUnprocessableEntity, errPayload("invalid request body: " + err.Error())
	}

	err = req.validate()
	if err != nil {
		return http.StatusUnprocessableEntity, errPayload(err.Error())
	}

	err = h.svc.UpdateRide(ctx, req.RideID)
	if err != nil {
		return mapError(err)
	}
	return http.StatusOK, ""
}

func mapError(err error) (int, string) {
	switch {
	case errors.Is(err, service.ErrNotFound):
		return http.StatusNotFound, errPayload(err.Error())
	case errors.Is(err, service.ErrConflict):
		return http.StatusConflict, errPayload(err.Error())
	case errors.Is(err, service.ErrUpstream):
		return http.StatusBadGateway, errPayload(err.Error())
	default:
		return http.StatusInternalServerError, errPayload(err.Error())
	}
}
