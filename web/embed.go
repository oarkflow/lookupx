package web

import "embed"

// Assets contains the LookupX browser console shipped with the server binary.
//
//go:embed index.html app.js styles.css
var Assets embed.FS
