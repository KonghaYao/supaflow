package platformops

import (
	"net/url"
	"os"
	"strconv"
	"strings"
)

var postgresURLQueryEnvironment = map[string]string{
	"application_name":     "PGAPPNAME",
	"channel_binding":      "PGCHANNELBINDING",
	"client_encoding":      "PGCLIENTENCODING",
	"connect_timeout":      "PGCONNECT_TIMEOUT",
	"gssencmode":           "PGGSSENCMODE",
	"keepalives":           "PGKEEPALIVES",
	"keepalives_count":     "PGKEEPALIVESCOUNT",
	"keepalives_idle":      "PGKEEPALIVESIDLE",
	"keepalives_interval":  "PGKEEPALIVESINTERVAL",
	"options":              "PGOPTIONS",
	"sslcert":              "PGSSLCERT",
	"sslcrl":               "PGSSLCRL",
	"sslkey":               "PGSSLKEY",
	"sslmode":              "PGSSLMODE",
	"sslrootcert":          "PGSSLROOTCERT",
	"target_session_attrs": "PGTARGETSESSIONATTRS",
	"tcp_user_timeout":     "PGTCPUSER_TIMEOUT",
}

// postgresToolEnvironment converts a URL-form DSN into explicit libpq
// variables. PostgreSQL command-line tools do not consistently expand a URI
// stored in PGDATABASE, so the verified destination must cross the process
// boundary as individual fields instead.
func postgresToolEnvironment(databaseURL string) ([]string, error) {
	parsed, err := url.Parse(strings.TrimSpace(databaseURL))
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") ||
		parsed.Opaque != "" || parsed.Host == "" || parsed.Hostname() == "" || parsed.Fragment != "" ||
		!validPostgresEnvironmentValue(parsed.Hostname()) {
		return nil, ErrInvalidArgument
	}
	databaseName := strings.TrimPrefix(parsed.Path, "/")
	if parsed.Path == "" || databaseName == "" || !validPostgresEnvironmentValue(databaseName) {
		return nil, ErrInvalidArgument
	}
	environment := []string{"PGHOST=" + parsed.Hostname(), "PGDATABASE=" + databaseName}
	if port := parsed.Port(); port != "" {
		value, conversionErr := strconv.ParseUint(port, 10, 16)
		if conversionErr != nil || value == 0 {
			return nil, ErrInvalidArgument
		}
		environment = append(environment, "PGPORT="+port)
	}
	if parsed.User != nil {
		username := parsed.User.Username()
		if username != "" {
			if !validPostgresEnvironmentValue(username) {
				return nil, ErrInvalidArgument
			}
			environment = append(environment, "PGUSER="+username)
		}
		if password, present := parsed.User.Password(); present {
			if !validPostgresEnvironmentValue(password) {
				return nil, ErrInvalidArgument
			}
			environment = append(environment, "PGPASSWORD="+password)
		}
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return nil, ErrInvalidArgument
	}
	for key, values := range query {
		environmentKey, supported := postgresURLQueryEnvironment[key]
		if !supported || len(values) != 1 || !validPostgresEnvironmentValue(values[0]) {
			return nil, ErrInvalidArgument
		}
		environment = append(environment, environmentKey+"="+values[0])
	}
	return environment, nil
}

func validPostgresEnvironmentValue(value string) bool {
	return !strings.ContainsAny(value, "\x00\r\n")
}

// postgresProcessEnvironment removes inherited libpq routing before applying
// the explicitly parsed URL. An inherited PGHOSTADDR could otherwise redirect
// a command even when PGHOST is correct.
func postgresProcessEnvironment(connection []string) []string {
	base := os.Environ()
	environment := make([]string, 0, len(base)+len(connection))
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(key, "PG") {
			environment = append(environment, entry)
		}
	}
	return append(environment, connection...)
}

func postgresEnvironmentSecrets(connection []string) []string {
	for _, entry := range connection {
		if password, found := strings.CutPrefix(entry, "PGPASSWORD="); found {
			return []string{password}
		}
	}
	return nil
}
