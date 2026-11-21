package webserver

import (
	"net/http"

	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
)

// Server is a web server that can serve webhooks and other HTTP handlers.
type Server interface {
	ctrlwebhook.Server

	// HostPort returns the host and port that the server is listening on.
	HostPort() (string, int, error)

	// NotFoundHandler sets the handler for requests that do not match any registered path.
	// If not set, the default http.NotFoundHandler will be used.
	NotFoundHandler(http.Handler)
}
