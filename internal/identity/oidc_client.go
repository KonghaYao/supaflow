package identity

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const maxOIDCResponseBytes = 1 << 20

type oidcDiscovery struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	JWKSURI               string `json:"jwks_uri"`
}

// OIDCConfig configures one OpenID Connect Authorization Code client.
type OIDCConfig struct {
	IssuerURL    string
	ClientID     string
	ClientSecret string
	RedirectURL  string
	HTTPClient   *http.Client
}

// VerifiedOIDCIdentity contains provider claims after signature and protocol validation.
type VerifiedOIDCIdentity struct {
	Claims OIDCClaims
	Nonce  string
}

// OIDCProvider is the login handler's narrow provider seam.
type OIDCProvider interface {
	AuthorizationURL(state, nonce, challenge string) string
	Exchange(context.Context, string, string) (VerifiedOIDCIdentity, error)
}

// DiscoveryOIDCProvider uses OIDC discovery, Authorization Code, PKCE, and signed ID tokens.
type DiscoveryOIDCProvider struct {
	config    OIDCConfig
	discovery oidcDiscovery
}

// DiscoverOIDCProvider loads and validates provider metadata. Missing or unsafe configuration fails closed.
func DiscoverOIDCProvider(ctx context.Context, config OIDCConfig) (*DiscoveryOIDCProvider, error) {
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	client := *config.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("OIDC redirects are disabled")
	}
	config.HTTPClient = &client
	issuer, err := secureURL(config.IssuerURL)
	if err != nil || strings.TrimSpace(config.ClientID) == "" {
		return nil, errors.New("OIDC issuer and client ID are required and issuer must use HTTPS")
	}
	redirect, err := secureURL(config.RedirectURL)
	if err != nil {
		return nil, errors.New("OIDC redirect URL is required and must use HTTPS")
	}
	config.IssuerURL = strings.TrimRight(issuer.String(), "/")
	config.RedirectURL = redirect.String()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, config.IssuerURL+"/.well-known/openid-configuration", nil)
	if err != nil {
		return nil, fmt.Errorf("create OIDC discovery request: %w", err)
	}
	var discovery oidcDiscovery
	if err = getOIDCJSON(config.HTTPClient, request, &discovery); err != nil {
		return nil, fmt.Errorf("discover OIDC provider: %w", err)
	}
	if discovery.Issuer != config.IssuerURL {
		return nil, errors.New("OIDC discovery issuer mismatch")
	}
	for _, endpoint := range []string{discovery.AuthorizationEndpoint, discovery.TokenEndpoint, discovery.JWKSURI} {
		if _, endpointErr := secureURL(endpoint); endpointErr != nil {
			return nil, errors.New("OIDC discovery returned an unsafe endpoint")
		}
	}
	return &DiscoveryOIDCProvider{config: config, discovery: discovery}, nil
}

// AuthorizationURL builds the provider redirect with PKCE S256 and nonce binding.
func (p *DiscoveryOIDCProvider) AuthorizationURL(state, nonce, challenge string) string {
	values := url.Values{
		"client_id":             {p.config.ClientID},
		"redirect_uri":          {p.config.RedirectURL},
		"response_type":         {"code"},
		"scope":                 {"openid profile email"},
		"state":                 {state},
		"nonce":                 {nonce},
		"code_challenge":        {challenge},
		"code_challenge_method": {"S256"},
	}
	return p.discovery.AuthorizationEndpoint + "?" + values.Encode()
}

// Exchange redeems an authorization code and validates the returned ID token.
func (p *DiscoveryOIDCProvider) Exchange(ctx context.Context, code, verifier string) (VerifiedOIDCIdentity, error) {
	values := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {p.config.RedirectURL},
		"code_verifier": {verifier},
	}
	if p.config.ClientSecret == "" {
		values.Set("client_id", p.config.ClientID)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.discovery.TokenEndpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return VerifiedOIDCIdentity{}, fmt.Errorf("create OIDC token request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if p.config.ClientSecret != "" {
		request.SetBasicAuth(p.config.ClientID, p.config.ClientSecret)
	}
	var tokens struct {
		IDToken string `json:"id_token"`
	}
	if err = getOIDCJSON(p.config.HTTPClient, request, &tokens); err != nil || tokens.IDToken == "" {
		return VerifiedOIDCIdentity{}, errors.New("OIDC token exchange failed")
	}
	return p.verifyIDToken(ctx, tokens.IDToken)
}

func (p *DiscoveryOIDCProvider) verifyIDToken(ctx context.Context, raw string) (VerifiedOIDCIdentity, error) {
	keys, err := p.loadKeys(ctx)
	if err != nil {
		return VerifiedOIDCIdentity{}, err
	}
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(raw, claims, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != "RS256" && token.Method.Alg() != "ES256" {
			return nil, errors.New("OIDC ID token signing algorithm is not allowed")
		}
		keyID, _ := token.Header["kid"].(string)
		key, ok := keys[keyID]
		if !ok || keyID == "" {
			return nil, errors.New("OIDC ID token signing key is unavailable")
		}
		return key, nil
	}, jwt.WithAudience(p.config.ClientID), jwt.WithIssuer(p.discovery.Issuer), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithValidMethods([]string{"RS256", "ES256"}))
	if err != nil || !token.Valid {
		return VerifiedOIDCIdentity{}, errors.New("OIDC ID token validation failed")
	}
	issuer, _ := claims["iss"].(string)
	subject, _ := claims["sub"].(string)
	nonce, _ := claims["nonce"].(string)
	if issuer == "" || subject == "" || nonce == "" {
		return VerifiedOIDCIdentity{}, errors.New("OIDC ID token lacks required claims")
	}
	audience, audienceErr := claims.GetAudience()
	if audienceErr != nil {
		return VerifiedOIDCIdentity{}, errors.New("OIDC ID token audience is invalid")
	}
	if len(audience) > 1 {
		authorizedParty, _ := claims["azp"].(string)
		if authorizedParty != p.config.ClientID {
			return VerifiedOIDCIdentity{}, errors.New("OIDC ID token authorized party is invalid")
		}
	}
	email, _ := claims["email"].(string)
	name, _ := claims["name"].(string)
	return VerifiedOIDCIdentity{Claims: OIDCClaims{Issuer: issuer, Subject: subject, Email: email, DisplayName: name}, Nonce: nonce}, nil
}

func (p *DiscoveryOIDCProvider) loadKeys(ctx context.Context) (map[string]any, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.discovery.JWKSURI, nil)
	if err != nil {
		return nil, err
	}
	var set struct {
		Keys []struct {
			KeyID string `json:"kid"`
			Type  string `json:"kty"`
			Curve string `json:"crv"`
			N     string `json:"n"`
			E     string `json:"e"`
			X     string `json:"x"`
			Y     string `json:"y"`
		} `json:"keys"`
	}
	if err = getOIDCJSON(p.config.HTTPClient, request, &set); err != nil {
		return nil, fmt.Errorf("load OIDC signing keys: %w", err)
	}
	keys := make(map[string]any, len(set.Keys))
	for _, item := range set.Keys {
		if key, keyErr := parseJWK(item.Type, item.Curve, item.N, item.E, item.X, item.Y); keyErr == nil && item.KeyID != "" {
			keys[item.KeyID] = key
		}
	}
	return keys, nil
}

func parseJWK(kind, curve, modulus, exponent, xValue, yValue string) (any, error) {
	decode := func(value string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(value) }
	switch kind {
	case "RSA":
		n, err := decode(modulus)
		if err != nil {
			return nil, err
		}
		e, err := decode(exponent)
		if err != nil || len(e) == 0 || len(e) > 4 {
			return nil, errors.New("invalid RSA exponent")
		}
		exponentValue := 0
		for _, value := range e {
			exponentValue = exponentValue<<8 + int(value)
		}
		modulusValue := new(big.Int).SetBytes(n)
		if modulusValue.BitLen() < 2048 || exponentValue < 3 || exponentValue%2 == 0 {
			return nil, errors.New("unsafe RSA signing key")
		}
		return &rsa.PublicKey{N: modulusValue, E: exponentValue}, nil
	case "EC":
		if curve != "P-256" {
			return nil, errors.New("unsupported EC curve")
		}
		x, err := decode(xValue)
		if err != nil {
			return nil, err
		}
		y, err := decode(yValue)
		if err != nil {
			return nil, err
		}
		xCoordinate, yCoordinate := new(big.Int).SetBytes(x), new(big.Int).SetBytes(y)
		if !elliptic.P256().IsOnCurve(xCoordinate, yCoordinate) {
			return nil, errors.New("invalid EC signing key")
		}
		return &ecdsa.PublicKey{Curve: elliptic.P256(), X: xCoordinate, Y: yCoordinate}, nil
	default:
		return nil, errors.New("unsupported JWK type")
	}
}

func secureURL(value string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, errors.New("URL must be an absolute HTTPS URL")
	}
	return parsed, nil
}

func getOIDCJSON(client *http.Client, request *http.Request, target any) error {
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxOIDCResponseBytes))
		return errors.New("OIDC endpoint returned an error")
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxOIDCResponseBytes))
	return decoder.Decode(target)
}
