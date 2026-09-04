// Package web embeds the production Studio bundle into the control-plane binary.
package web

import (
	"embed"
	"io/fs"
)

//go:embed dist
var bundled embed.FS

// Assets returns the immutable built Studio filesystem rooted at dist.
func Assets() fs.FS {
	assets, err := fs.Sub(bundled, "dist")
	if err != nil {
		panic("embedded Studio distribution is unavailable")
	}
	return assets
}
