package service

import (
	"context"
	"database/sql"
)

type Dependencies interface {
	BeginRideSession(ctx context.Context, rideID string) (*sql.Tx, error)
	GetRide(ctx context.Context, tx *sql.Tx, rideID string) (Ride, error)
	SaveResult(ctx context.Context, tx *sql.Tx, rideID, driverName string) error
	FetchDriver(ctx context.Context, driverID string) (Driver, error)
}
