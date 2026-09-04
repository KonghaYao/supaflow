package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/api"
	"github.com/pazhou/supaflow/internal/identity"
)

func configureIdentity(ctx context.Context, pool *pgxpool.Pool) (*api.Authenticator, *identity.HTTPHandler, error) {
	if strings.TrimSpace(os.Getenv("SUPAFLOW_API_KEYS_JSON")) != "" {
		return nil, nil, errors.New("SUPAFLOW_API_KEYS_JSON is no longer accepted; use explicit one-time bootstrap configuration")
	}
	store, err := identity.NewPostgresStore(pool)
	if err != nil {
		return nil, nil, err
	}
	if err = bootstrapIdentity(ctx, store); err != nil {
		return nil, nil, err
	}
	authenticator, err := api.NewDatabaseAuthenticator(store)
	if err != nil {
		return nil, nil, err
	}
	if !enabled(os.Getenv("SUPAFLOW_OIDC_ENABLED")) {
		return authenticator, nil, nil
	}
	provider, err := identity.DiscoverOIDCProvider(ctx, identity.OIDCConfig{
		IssuerURL: strings.TrimSpace(os.Getenv("SUPAFLOW_OIDC_ISSUER_URL")),
		ClientID:  strings.TrimSpace(os.Getenv("SUPAFLOW_OIDC_CLIENT_ID")), ClientSecret: os.Getenv("SUPAFLOW_OIDC_CLIENT_SECRET"),
		RedirectURL: strings.TrimSpace(os.Getenv("SUPAFLOW_OIDC_REDIRECT_URL")),
	})
	if err != nil {
		return nil, nil, err
	}
	handler, err := identity.NewHTTPHandler(store, provider, identity.HTTPConfig{
		LoginTTL: 10 * time.Minute, SessionTTL: 8 * time.Hour,
		SuccessRedirect: strings.TrimSpace(os.Getenv("SUPAFLOW_OIDC_SUCCESS_REDIRECT")),
	})
	if err != nil {
		return nil, nil, err
	}
	return authenticator, handler, nil
}

func bootstrapIdentity(ctx context.Context, store *identity.PostgresStore) error {
	value := strings.TrimSpace(os.Getenv("SUPAFLOW_BOOTSTRAP_API_KEYS_JSON"))
	if !enabled(os.Getenv("SUPAFLOW_IDENTITY_BOOTSTRAP_ENABLED")) {
		if value != "" {
			return errors.New("SUPAFLOW_BOOTSTRAP_API_KEYS_JSON requires SUPAFLOW_IDENTITY_BOOTSTRAP_ENABLED=true")
		}
		return nil
	}
	keys, err := parseAPIKeys(value)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err = store.BootstrapAPIKey(ctx, identity.BootstrapAPIKey{
			Value: key.Value, TenantID: key.Principal.TenantID, ActorID: key.Principal.ActorID,
			Name: "bootstrap-" + key.Principal.ActorID, Role: key.Principal.Role,
		}); err != nil {
			return err
		}
	}
	return nil
}

func enabled(value string) bool { return strings.EqualFold(strings.TrimSpace(value), "true") }
