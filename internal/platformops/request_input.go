package platformops

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// unstorableTextCode is the Postgres SQLSTATE for text it refuses to store: a NUL byte or a
	// byte sequence that is not valid UTF-8.
	unstorableTextCode = "22021"
	// unstorableJSONCode is the Postgres SQLSTATE for a jsonb input whose escape sequences it
	// cannot convert into text, which today means the escape \u0000.
	unstorableJSONCode = "22P05"
	// invalidTextRepresentationCode is Postgres' generic "the input parser rejected this text".
	// Every input type shares it, so it is only read as a caller error when the message names json.
	invalidTextRepresentationCode = "22P02"
)

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

// StorableJSON reports whether JSON text survives a round trip through a Postgres jsonb column.
//
// Go's encoding/json is more permissive than the jsonb input parser: it accepts the escape \u0000,
// which Postgres cannot convert into text (22P05), and unpaired surrogate escapes such as \ud800,
// which its parser rejects as invalid json input (22P02). Both are the caller's bytes, so an HTTP
// edge answers them with a client error instead of letting them reach a column and come back as a
// storage failure. The scan is a lexer rather than a decode because a decoded json.RawMessage
// keeps the caller's escapes verbatim, and those bytes are what Postgres parses.
//
// Only escapes inside string literals are interpreted, and an escape is consumed together with
// the character it escapes, so the literal text \\ followed by u0000 — a backslash, which jsonb
// stores happily — is not mistaken for a NUL escape, and \" does not end the string early.
//
// A JSON document is UTF-8 by definition (RFC 8259), so a body that is not valid UTF-8 has no
// decoded form to preserve: encoding/json substitutes U+FFFD for every illegal byte without any
// error, which makes two different requests (…-a\xff and …-a\xfe) decode to the same text and
// store the same value. The second one is then answered "already exists" for a name the caller
// never asked for, and the name it did ask for becomes impossible to create. Rejecting the bytes
// keeps naming honest; it is also the same judgement StorableText already makes for the request
// target, so the body and the target stay symmetric.
func StorableJSON(value []byte) bool {
	if !utf8.Valid(value) {
		return false
	}
	insideString := false
	for index := 0; index < len(value); index++ {
		switch value[index] {
		case '"':
			insideString = !insideString
		case '\\':
			if !insideString {
				continue
			}
			storable, length := storableEscape(value, index)
			if !storable {
				return false
			}
			index += length - 1
		}
	}
	return true
}

// storableEscape reports whether the escape sequence starting at the backslash at start denotes a
// character jsonb stores, and how many bytes it occupies. The length is what the caller must skip,
// because an accepted surrogate pair spans two escapes.
func storableEscape(value []byte, start int) (bool, int) {
	if start+1 >= len(value) {
		return false, len(value) - start
	}
	if value[start+1] != 'u' {
		return true, 2 // \\ \" \/ \b \f \n \r \t all denote characters jsonb stores
	}
	code, ok := unicodeEscape(value[start+2:])
	if !ok {
		return false, len(value) - start
	}
	switch {
	case code == 0:
		return false, 6 // \u0000: "unsupported Unicode escape sequence ... cannot be converted to text"
	case !utf16.IsSurrogate(rune(code)):
		return true, 6 // \u001f, \uffff and every ordinary code point are stored as they are
	case code > 0xDBFF:
		return false, 6 // a low surrogate on its own never follows a high surrogate
	}
	// A high surrogate is storable only when the very next escape is the low surrogate that pairs
	// with it, i.e. \uXXXX\uYYYY — twelve bytes, whose last four are the low surrogate's digits.
	// Anything else — another high surrogate, an ordinary escape, the end of the text — is a
	// sequence Postgres reads as "a low surrogate must follow a high surrogate".
	if start+12 > len(value) {
		return false, len(value) - start
	}
	low, ok := unicodeEscape(value[start+8:])
	if !ok || low < 0xDC00 || low > 0xDFFF {
		return false, 6
	}
	return true, 12
}

// unicodeEscape reads the four hexadecimal digits of a \u escape, which is where digits points.
func unicodeEscape(digits []byte) (uint32, bool) {
	if len(digits) < 4 {
		return 0, false
	}
	code, err := strconv.ParseUint(string(digits[:4]), 16, 32)
	if err != nil {
		return 0, false
	}
	return uint32(code), true
}

// IsUnstorableText reports whether err is Postgres refusing bytes the request supplied: text it
// cannot encode (22021), a jsonb input whose escapes it cannot convert (22P05), or json syntax its
// parser rejects (22P02).
//
// Such bytes reach a column only from a request that carried them, so the failure belongs to the
// caller. Without this classification it surfaced as a plain storage error and the API answered
// 500 internal_error, which told the caller to retry a request that can never succeed. Values
// taken from the request target are already rejected at the HTTP edge; this covers the ones nested
// in a request body.
//
// 22P02 is the generic "invalid text representation" code that integers, timestamps and json all
// share, so it counts only when the message names the json input: a 22P02 from anywhere else, or
// with no message at all, stays a server-side failure rather than being reported as a client error.
func IsUnstorableText(err error) bool {
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) {
		return false
	}
	switch pgError.Code {
	case unstorableTextCode, unstorableJSONCode:
		return true
	case invalidTextRepresentationCode:
		return strings.Contains(pgError.Message, "for type json")
	}
	return false
}
