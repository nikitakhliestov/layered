package service

import "context"

type Dependencies interface {
	RunInNewTx(ctx context.Context, f func(ctx context.Context, tx AtomicDeps) error) error
}

//nolint:interfacebloat // transaction dependency interface, splitting would reduce atomicity guarantees
type AtomicDeps interface {
	Lock(ctx context.Context, rideID string) error
	GetRide(ctx context.Context, rideID string) (Ride, error)
	FetchDriver(ctx context.Context, driverID string) (Driver, error)
	Persist(ctx context.Context, rideID, driverName string) error
}
