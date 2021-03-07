package controller

import (
	"net/http"

	handler "gitlab.inspr.dev/inspr/core/cmd/insprd/api/handlers"
)

func (s *Server) initRoutes() {
	h := handler.Handler{Memory: s.MemoryManager, Operator: s.op}

	ahandler := handler.NewAppHandler(h)
	s.Mux.HandleFunc("/apps", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {

		case http.MethodGet:
			ahandler.HandleGetAppByRef().JSON().Recover()(w, r)

		case http.MethodPost:
			ahandler.HandleCreateApp().JSON().Recover()(w, r)

		case http.MethodPut:
			ahandler.HandleUpdateApp().JSON().Recover()(w, r)

		case http.MethodDelete:
			ahandler.HandleDeleteApp().JSON().Recover()(w, r)

		default:
			http.Error(w, "405 method not allowed", http.StatusMethodNotAllowed)
			return
		}
	})

	chandler := handler.NewChannelHandler(h)
	s.Mux.HandleFunc("/channels", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {

		case http.MethodGet:
			chandler.HandleGetChannelByRef().JSON().Recover()(w, r)

		case http.MethodPost:
			chandler.HandleCreateChannel().JSON()(w, r)

		case http.MethodPut:
			chandler.HandleUpdateChannel().JSON().Recover()(w, r)

		case http.MethodDelete:
			chandler.HandleDeleteChannel().JSON().Recover()(w, r)

		default:
			http.Error(w, "405 method not allowed", http.StatusMethodNotAllowed)
			return
		}
	})

	cthandler := handler.NewChannelTypeHandler(h)
	s.Mux.HandleFunc("/channeltypes", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {

		case http.MethodGet:
			cthandler.HandleGetChannelTypeByRef().JSON().Recover()(w, r)

		case http.MethodPost:
			cthandler.HandleCreateChannelType().JSON()(w, r)

		case http.MethodPut:
			cthandler.HandleUpdateChannelType().JSON().Recover()(w, r)

		case http.MethodDelete:
			cthandler.HandleDeleteChannelType().JSON().Recover()(w, r)

		default:
			http.Error(w, "405 method not allowed", http.StatusMethodNotAllowed)
			return
		}
	})
}
