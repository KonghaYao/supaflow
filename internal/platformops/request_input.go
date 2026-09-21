package platformops

import (
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
)

// unstorableTextCode is the Postgres SQLSTATE for text it refuses to store: a NUL byte or a
// byte sequence that is not valid UTF-8.
const unstorableTextCode = "22021"

// ValidRequestTarget reports whether every value the request carries in its path and query is
// text the platform can store and query for. Both rejections are the caller's, so an HTTP edge
// answers them with one client error instead of letting them reach a query.
func ValidRequestTarget(r *http.Request) bool {
	if !StorableText(r.URL.Path) {
		return false
	}
	for _, values := range r.URL.Query() {
		for _, value := range values {
			if !StorableText(value) {
				return false
			}
		}
	}
	return true
}

// StorableText reports whether value survives a round trip through a Postgres text column.
func StorableText(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

// IsUnstorableText reports whether err is Postgres rejecting bytes it cannot store as text.
//
// Such bytes reach a column only from a request that carried them, so the failure belongs to
// the caller. Without this classification it surfaced as a plain storage error and the API
// answered 500 internal_error, which told the caller to retry a request that can never succeed.
// Values taken from the request target are already rejected at the HTTP edge; this covers the
// ones nested in a request body.
func IsUnstorableText(err error) bool {
	var pgError *pgconn.PgError
	return errors.As(err, &pgError) && pgError.Code == unstorableTextCode
}
