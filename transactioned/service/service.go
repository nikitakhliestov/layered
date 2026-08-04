package service

import (
	"context"

	"github.com/pkg/errors"
)

type Service struct {
	deps Dependencies
}

func New(deps Dependencies) *Service {
	return &Service{deps: deps}
}

func (s *Service) UpdateRide(ctx context.Context, rideID string) error {
	err := s.deps.RunInNewTx(ctx, func(ctx context.Context, tx AtomicDeps) error {
		return updateRideInTx(ctx, tx, rideID)
	})
	if err != nil {
		return errors.WithMessage(err, "run in tx")
	}

	return nil
}

func updateRideInTx(ctx context.Context, tx AtomicDeps, rideID string) error {
	err := tx.Lock(ctx, rideID)
	if err != nil {
		return errors.WithMessage(err, "lock ride")
	}

	ride, err := tx.GetRide(ctx, rideID)
	if err != nil {
		return err
	}

	if ride.Processed {
		return nil
	}

	driver, err := tx.FetchDriver(ctx, ride.DriverID)
	if err != nil {
		return err
	}

	if !driver.IsActive {
		return errors.WithMessagef(ErrConflict, "driver %s is not active", driver.ID)
	}

	err = tx.Persist(ctx, rideID, driver.Name)
	if err != nil {
		return errors.WithMessage(err, "persist result")
	}

	return nil
}
