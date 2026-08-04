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
	// Transaction (will be reworked)
	tx, err := s.deps.BeginRideSession(ctx, rideID)
	if err != nil {
		return errors.WithMessagef(err, "begin ride session")
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	ride, err := s.deps.GetRide(ctx, tx, rideID)
	if err != nil {
		return err
	}

	// Idempotency check
	if ride.Processed {
		committed = true
		return nil
	}

	// External service call
	driver, err := s.deps.FetchDriver(ctx, ride.DriverID)
	if err != nil {
		return err
	}

	if !driver.IsActive {
		return errors.WithMessagef(ErrConflict, "driver %s is not active", driver.ID)
	}

	// Persist
	err = s.deps.SaveResult(ctx, tx, rideID, driver.Name)
	if err != nil {
		return errors.WithMessagef(err, "save result")
	}

	// Transaction finalization
	err = tx.Commit()
	if err != nil {
		return errors.WithMessagef(err, "commit")
	}
	committed = true

	return nil
}
