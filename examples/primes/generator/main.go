package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	dappclient "inspr.dev/inspr/pkg/client"
	"inspr.dev/inspr/pkg/sidecar/models"
)

const defaultMOD = 100

func main() {
	var mod int

	// reads from env
	modString, exists := os.LookupEnv("MODULE")
	if !exists {
		mod = defaultMOD
	} else {
		mod, _ = strconv.Atoi(modString)
	}

	// sets up ticker and rand
	ticker := time.NewTicker(2 * time.Second)
	rand.Seed(time.Now().UnixNano())

	// sets up client for sidecar
	c := dappclient.NewAppClient()
	// channelName
	chName := "primes_ch1"
	ctx := context.Background()
	fmt.Println("starting...")
	for range ticker.C {
		randNumber := rand.Int() % mod
		fmt.Println("random number -> ", randNumber)
		newMsg := models.Message{
			Data: randNumber,
		}

		err := c.WriteMessage(ctx, chName, newMsg)
		if err != nil {
			fmt.Println(err.Error())
		}
	}
}
