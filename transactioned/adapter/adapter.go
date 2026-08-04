package adapter

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/pkg/errors"

	"layered/transactioned/service"
)

type Adapter struct {
	db               *sql.DB
	httpClient       *http.Client
	driverServiceURL string
}

func New(db *sql.DB, httpClient *http.Client, driverServiceURL string) *Adapter {
	return &Adapter{db: db, httpClient: httpClient, driverServiceURL: driverServiceURL}
}

var _ service.Dependencies = (*Adapter)(nil)

func (a *Adapter) RunInNewTx(ctx context.Context, f func(ctx context.Context, tx service.TxDeps) error) error {
	sqlTx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "begin tx")
	}

	err = f(ctx, &txDeps{tx: sqlTx, httpClient: a.httpClient, driverServiceURL: a.driverServiceURL})
	if err != nil {
		_ = sqlTx.Rollback()
		return err
	}

	err = sqlTx.Commit()
	if err != nil {
		return errors.Wrap(err, "commit")
	}

	return nil
}
