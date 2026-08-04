package adapter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"

	"layered/transactioned/service"
)

type atomicDeps struct {
	tx               *sql.Tx
	httpClient       *http.Client
	driverServiceURL string
}

var _ service.AtomicDeps = (*atomicDeps)(nil)

func (td *atomicDeps) Lock(ctx context.Context, rideID string) error {
	_, err := td.tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, rideID)
	if err != nil {
		return errors.Wrapf(err, "lock ride %s", rideID)
	}

	return nil
}

func (td *atomicDeps) GetRide(ctx context.Context, rideID string) (service.Ride, error) {
	var (
		ride          service.Ride
		processedAt   sql.NullTime
		resultPayload sql.NullString
	)

	err := td.tx.QueryRowContext(ctx,
		`SELECT id, driver_id, processed_at, result_payload FROM rides WHERE id = $1`, rideID,
	).Scan(&ride.ID, &ride.DriverID, &processedAt, &resultPayload)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return service.Ride{}, errors.Wrapf(service.ErrNotFound, "ride %s", rideID)
		}
		return service.Ride{}, errors.Wrap(err, "query ride")
	}

	ride.Processed = processedAt.Valid
	ride.ResultPayload = resultPayload.String

	return ride, nil
}

func (td *atomicDeps) FetchDriver(ctx context.Context, driverID string) (service.Driver, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/drivers/%s", td.driverServiceURL, driverID), nil)
	if err != nil {
		return service.Driver{}, errors.Wrap(err, "build driver request")
	}

	resp, err := td.httpClient.Do(req)
	if err != nil {
		return service.Driver{}, errors.Wrapf(service.ErrUpstream, "driver service unreachable: %s", err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return service.Driver{}, errors.Wrapf(service.ErrUpstream, "read driver response: %s", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return service.Driver{}, errors.Wrapf(service.ErrNotFound, "driver %s", driverID)
	}

	if resp.StatusCode != http.StatusOK {
		return service.Driver{}, errors.Wrapf(service.ErrUpstream, "driver service returned %d", resp.StatusCode)
	}

	var raw struct {
		DriverID string `json:"driver_id"`
		Name     string `json:"name"`
		IsActive bool   `json:"is_active"`
	}

	err = json.Unmarshal(body, &raw)
	if err != nil {
		return service.Driver{}, errors.Wrapf(service.ErrUpstream, "invalid driver response: %s", err)
	}

	if raw.DriverID == "" {
		return service.Driver{}, errors.Wrapf(service.ErrNotFound, "driver %s", driverID)
	}

	return service.Driver{ID: raw.DriverID, Name: raw.Name, IsActive: raw.IsActive}, nil
}

func (td *atomicDeps) Persist(ctx context.Context, rideID, driverName string) error {
	payload, err := json.Marshal(struct {
		RideID     string `json:"ride_id"`
		DriverName string `json:"driver_name"`
	}{RideID: rideID, DriverName: driverName})
	if err != nil {
		return errors.Wrap(err, "marshal result")
	}

	_, err = td.tx.ExecContext(ctx,
		`UPDATE rides SET status = 'processed', processed_at = $1, result_payload = $2
		 WHERE id = $3 AND processed_at IS NULL`,
		time.Now().UTC(), string(payload), rideID,
	)
	if err != nil {
		return errors.Wrap(err, "persist result")
	}

	return nil
}
