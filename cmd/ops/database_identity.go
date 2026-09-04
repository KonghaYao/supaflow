package main

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type databaseIdentity struct {
	SystemIdentifier string
	DatabaseOID      uint32
	DatabaseName     string
}

type databaseIdentityReader func(context.Context, string) (databaseIdentity, error)

// readPostgresDatabaseIdentity resolves aliases through PostgreSQL itself. The
// cluster system identifier and catalog OID cannot be changed by DSN spelling.
func readPostgresDatabaseIdentity(ctx context.Context, databaseURL string) (databaseIdentity, error) {
	queryContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return databaseIdentity{}, errors.New("database connection configuration is invalid")
	}
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(queryContext, config)
	if err != nil {
		return databaseIdentity{}, errors.New("database identity connection failed")
	}
	defer pool.Close()
	var identity databaseIdentity
	err = pool.QueryRow(queryContext, `SELECT control.system_identifier::text,
		database.oid::bigint,current_database()
		FROM pg_control_system() control
		JOIN pg_database database ON database.datname=current_database()`).Scan(
		&identity.SystemIdentifier,
		&identity.DatabaseOID,
		&identity.DatabaseName,
	)
	if err != nil || identity.SystemIdentifier == "" || identity.DatabaseOID == 0 || identity.DatabaseName == "" {
		return databaseIdentity{}, errors.New("database identity could not be verified")
	}
	return identity, nil
}

func verifySeparateRestoreDatabase(
	ctx context.Context,
	productionURL string,
	scratchURL string,
	readIdentity databaseIdentityReader,
) error {
	production, err := readIdentity(ctx, productionURL)
	if err != nil {
		return err
	}
	scratch, err := readIdentity(ctx, scratchURL)
	if err != nil {
		return err
	}
	if production == scratch {
		return errors.New("SUPAFLOW_RESTORE_DATABASE_URL must identify a separate scratch database")
	}
	return nil
}

func runVerifiedRestore(
	ctx context.Context,
	productionURL string,
	scratchURL string,
	readIdentity databaseIdentityReader,
	restore func() error,
) error {
	// Resolve once during validation and once immediately before the external
	// pg_restore connection, failing closed on DNS/alias/configuration changes.
	if err := verifySeparateRestoreDatabase(ctx, productionURL, scratchURL, readIdentity); err != nil {
		return err
	}
	if err := verifySeparateRestoreDatabase(ctx, productionURL, scratchURL, readIdentity); err != nil {
		return err
	}
	return restore()
}
