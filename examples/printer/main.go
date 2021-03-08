package main

import (
	"context"
	"fmt"
	"log"
	"time"

	dappclient "gitlab.inspr.dev/inspr/core/pkg/client"
)

func main() {
	// sets up client for sidecar
	c := dappclient.NewAppClient()

	// sets up ticker
	ticker := time.NewTicker(2 * time.Second)
	ctx := context.Background()

	for {
		select {
		case <-ticker.C:
			message, err := c.ReadMessage(ctx, "ch1")
			if err != nil {
				log.Println(err.Error())
			}

			fmt.Println("Message -> ", message)
			fmt.Println("Message Content -> ", message.Data)

			err = c.CommitMessage(ctx, "ch1")
			if err != nil {
				log.Println(err.Error())
			}

		}
	}
}
