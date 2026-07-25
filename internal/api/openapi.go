package api

import (
	_ "embed"
	"net/http"
)

// The API document is hand-written and embedded rather than generated from
// annotations. Two reasons, and the second is the real one.
//
// Generators describe what the handlers happen to return. Half of what a consumer of
// this service needs to know is not in a struct: that a request without `snapshot`
// gets `latest`, that a cursor pins the version it came from, that a diff's two sides
// reconstruct a dump when applied in order, that `409` carries the head to jump to.
// Those are contract, and they are written down here.
//
// The risk of hand-writing is drift, so a test holds the document to the router:
// every route must be in the spec and every spec path must be a route. Descriptions
// can still go stale, but the surface cannot.

//go:embed openapi.yaml
var openAPISpec []byte

// handleOpenAPISpec serves the document.
func handleOpenAPISpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Write(openAPISpec)
}

// docsPage renders the document with Swagger UI.
//
// Loaded from a CDN, which is a deliberate trade: the alternative is vendoring about
// a megabyte of JavaScript into this repo. The page degrades honestly — if the CDN is
// unreachable it says so and points at the raw document, which is the thing that
// actually matters and is served locally.
const docsPage = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Lido Keys API (versioned)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
  <style>
    body { margin: 0; font: 16px/1.5 system-ui, sans-serif; }
    #offline { display: none; padding: 2rem 2.5rem; max-width: 40rem; }
    #offline code { background: #f4f4f5; padding: 0.1em 0.35em; border-radius: 3px; }
  </style>
</head>
<body>
  <div id="offline">
    <h1>Swagger UI could not be loaded</h1>
    <p>The renderer comes from a CDN and this host cannot reach it. The document
    itself is served from here and is complete without the renderer:</p>
    <p><a href="/openapi.yaml"><code>/openapi.yaml</code></a></p>
  </div>
  <div id="swagger"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"
          onerror="document.getElementById('offline').style.display='block'"></script>
  <script>
    window.addEventListener('load', function () {
      if (typeof SwaggerUIBundle !== 'function') {
        document.getElementById('offline').style.display = 'block';
        return;
      }
      SwaggerUIBundle({
        url: '/openapi.yaml',
        dom_id: '#swagger',
        docExpansion: 'list',
        defaultModelsExpandDepth: 0,
        tryItOutEnabled: true,
      });
    });
  </script>
</body>
</html>
`

// handleDocs serves the rendering page.
func handleDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(docsPage))
}
