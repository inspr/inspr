/*
Package rest contains the functions
that make it easier to manager api
handler functions
*/
package rest

import (
	"net/http"

	"go.uber.org/zap"
	"inspr.dev/inspr/pkg/logs"
	"inspr.dev/inspr/pkg/meta/utils"
)

// Healthz handles health check requests
func Healthz() Handler {
	l, _ := logs.Logger(zap.Fields(zap.String("section", "healthcheck")))
	return func(w http.ResponseWriter, r *http.Request) {
		l.Info("received heathcheck - responding with 200 OK")
		w.Write([]byte("OK"))
	}
}

// Handler is an alias of the api router function.
// It acts as the function that handles the routes but at
// the same time it contains certain methods attached to it
// that allows for more utility. For an example check the JSON function.
type Handler func(w http.ResponseWriter, r *http.Request)

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h(w, r)
}

// HTTPHandlerFunc transforms the rest.handler to a http.handlerFunc type
func (h Handler) HTTPHandlerFunc() http.HandlerFunc {
	return http.HandlerFunc(h)
}

// Get runs function if the request method is GET
func (h Handler) Get() Handler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "405 method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

// Post runs function if the request method is POST
func (h Handler) Post() Handler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "405 method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

// Delete runs function if the request method is DELETE
func (h Handler) Delete() Handler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.Header().Set("Allow", http.MethodDelete)
			http.Error(w, "405 method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

// Put runs function if the request method is PUT
func (h Handler) Put() Handler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.Header().Set("Allow", http.MethodPut)
			http.Error(w, "405 method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

//Recover adds a recover function to the handler
func (h Handler) Recover(execOnRecover ...func()) Handler {
	return func(w http.ResponseWriter, r *http.Request) {
		defer RecoverFromPanic(w, execOnRecover...)
		h(w, r)
	}
}

// Methods allows for generic method definition
func (h Handler) Methods(methods ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		set, _ := utils.MakeStrSet(methods)
		if !set[r.Method] {
			http.Error(w, "405 method not allowed", http.StatusMethodNotAllowed)
			return
		}

		h(w, r)
	}
}
