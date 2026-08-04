package rides

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"
)

type UpdateRideRequest struct {
	RideID string `json:"ride_id"`
}

var rideIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

func (r UpdateRideRequest) Validate() error {
	if r.RideID == "" {
		return errors.New("ride_id is required")
	}
	if !rideIDPattern.MatchString(r.RideID) {
		return errors.New("ride_id has invalid format")
	}
	return nil
}

type rideRow struct {
	ID            string
	DriverID      string
	Status        string
	ProcessedAt   sql.NullTime
	ResultPayload sql.NullString
}

type DriverInfo struct {
	DriverID string `json:"driver_id"`
	Name     string `json:"name"`
	IsActive bool   `json:"is_active"`
}

type UpdateRideResponse struct {
	RideID     string `json:"ride_id"`
	DriverName string `json:"driver_name"`
}

func errPayload(msg string) string {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return string(b)
}

// HandleUpdateRide parses reqBody, validates, then takes a per-ride advisory lock for the
// duration of a single tx covering read, driverService call, and result write.
// ponytail: still one func on purpose — split into port/service/adapter next step.
func HandleUpdateRide(ctx context.Context, db *sql.DB, httpClient *http.Client, driverServiceURL, reqBody string) (int, string) {
	// 0/1. parse + validate -> 422
	var req UpdateRideRequest
	if err := json.Unmarshal([]byte(reqBody), &req); err != nil {
		return http.StatusUnprocessableEntity, errPayload("invalid request body: " + err.Error())
	}
	if err := req.Validate(); err != nil {
		return http.StatusUnprocessableEntity, errPayload(err.Error())
	}

	// tx + advisory lock by id — held until commit/rollback, serializes concurrent requests for same ride_id
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return http.StatusInternalServerError, errPayload("begin tx: " + err.Error())
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, req.RideID); err != nil {
		return http.StatusInternalServerError, errPayload("acquire lock: " + err.Error())
	}

	// 2. read ride by id + idempotency check
	var row rideRow
	err = tx.QueryRowContext(ctx,
		`SELECT id, driver_id, status, processed_at, result_payload FROM rides WHERE id = $1`,
		req.RideID,
	).Scan(&row.ID, &row.DriverID, &row.Status, &row.ProcessedAt, &row.ResultPayload)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return http.StatusNotFound, errPayload("ride not found")
		}
		return http.StatusInternalServerError, errPayload("query ride: " + err.Error())
	}
	if row.ProcessedAt.Valid {
		if err := tx.Commit(); err != nil {
			return http.StatusInternalServerError, errPayload("commit: " + err.Error())
		}
		committed = true
		// already processed — replay stored result, idempotent 200
		return http.StatusOK, row.ResultPayload.String
	}

	// 3. call driverService
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/drivers/%s", driverServiceURL, row.DriverID), nil)
	if err != nil {
		return http.StatusInternalServerError, errPayload("build driver request: " + err.Error())
	}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return http.StatusBadGateway, errPayload("driver service unreachable: " + err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return http.StatusBadGateway, errPayload("read driver response: " + err.Error())
	}
	if resp.StatusCode == http.StatusNotFound {
		return http.StatusNotFound, errPayload("driver not found")
	}
	if resp.StatusCode != http.StatusOK {
		return http.StatusBadGateway, errPayload(fmt.Sprintf("driver service returned %d: %s", resp.StatusCode, string(body)))
	}

	var driver DriverInfo
	if err := json.Unmarshal(body, &driver); err != nil {
		return http.StatusBadGateway, errPayload("invalid driver response: " + err.Error())
	}
	if driver.DriverID == "" {
		return http.StatusNotFound, errPayload("driver not found")
	}
	if !driver.IsActive {
		return http.StatusConflict, errPayload("driver is not active")
	}

	// 4. save result on same row — advisory lock already serializes writers, guard stays as defense in depth
	respPayload, _ := json.Marshal(UpdateRideResponse{RideID: row.ID, DriverName: driver.Name})
	res, err := tx.ExecContext(ctx,
		`UPDATE rides SET status = 'processed', processed_at = $1, result_payload = $2
		 WHERE id = $3 AND processed_at IS NULL`,
		time.Now().UTC(), string(respPayload), row.ID,
	)
	if err != nil {
		return http.StatusInternalServerError, errPayload("save result: " + err.Error())
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		var stored sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT result_payload FROM rides WHERE id = $1`, row.ID).Scan(&stored); err != nil {
			return http.StatusInternalServerError, errPayload("reload after race: " + err.Error())
		}
		if err := tx.Commit(); err != nil {
			return http.StatusInternalServerError, errPayload("commit: " + err.Error())
		}
		committed = true
		return http.StatusOK, stored.String
	}

	if err := tx.Commit(); err != nil {
		return http.StatusInternalServerError, errPayload("commit: " + err.Error())
	}
	committed = true
	return http.StatusOK, string(respPayload)
}
