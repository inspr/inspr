package dappclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"inspr.dev/inspr/pkg/ierrors"
	"inspr.dev/inspr/pkg/rest"
	"inspr.dev/inspr/pkg/rest/request"
	"inspr.dev/inspr/pkg/sidecars/models"
)

// Client is the struct which implements the methods of AppClient interface
type Client struct {
	client   *request.Client
	mux      *http.ServeMux
	readAddr string
}

// NewAppClient returns a new instance of the client of the AppClient package
func NewAppClient() *Client {

	writeAddr := fmt.Sprintf("http://localhost:%s", os.Getenv("INSPR_LBSIDECAR_WRITE_PORT"))
	readAddr := fmt.Sprintf(":%s", os.Getenv("INSPR_SCCLIENT_READ_PORT"))
	return &Client{
		readAddr: readAddr,
		client: request.NewClient().
			BaseURL(writeAddr).
			Encoder(json.Marshal).
			Decoder(request.JSONDecoderGenerator).
			Pointer(),
		mux: http.NewServeMux(),
	}
}

// WriteMessage receives a channel and a message and sends it in a request to the sidecar server
func (c *Client) WriteMessage(ctx context.Context, channel string, msg interface{}) error {
	data := models.BrokerMessage{
		Data: msg,
	}

	var resp interface{}
	log.Println("sending message to sidecar")
	// sends a message to the corresponding channel route on the sidecar
	err := c.client.Send(ctx, "/"+channel, http.MethodPost, data, &resp)
	log.Println("message sent")
	return err
}

// HandleChannel handles messages received in a given channel.
func (c *Client) HandleChannel(channel string, handler func(ctx context.Context, body io.Reader) error) {
	c.mux.HandleFunc("/"+channel, func(w http.ResponseWriter, r *http.Request) {
		// user defined handler. Returns error if the user wants to return it
		err := handler(context.Background(), r.Body)
		if err != nil {
			rest.ERROR(w, ierrors.NewError().InternalServer().InnerError(err).Build())
			return
		}
		rest.JSON(w, 200, nil)
	})
}

//Run runs the server with the handlers defined in HandleChannel
func (c *Client) Run(ctx context.Context) error {

	var err error
	server := http.Server{
		Handler: c.mux,
		Addr:    c.readAddr,
	}

	go func() {
		if err = server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen:%v", err)
		}
	}()

	log.Printf("dApp client listener is up...")

	<-ctx.Done()

	log.Println("gracefully shutting down...")

	ctxShutdown, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(time.Second*5),
	)
	defer cancel()

	if err != nil {
		log.Fatal(err)
	}

	// has to be the last method called in the shutdown
	if err = server.Shutdown(ctxShutdown); err != nil {
		return err
	}
	return ctx.Err()
}
