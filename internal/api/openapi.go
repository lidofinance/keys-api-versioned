package api

import (
	_ "embed"
	"net/http"
)

// Hand-written, not generated: much of the contract (default pin, cursor pinning, 409 payload) is
// not in a struct. A test holds the document's paths to the router.

//go:embed openapi.yaml
var openAPISpec []byte

func handleOpenAPISpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Write(openAPISpec)
}

// Swagger UI from a CDN rather than ~1 MB vendored JS; if it fails, the page links the raw document.
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

func handleDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(docsPage))
}
