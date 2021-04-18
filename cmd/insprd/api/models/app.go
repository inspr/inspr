package models

import (
	"inspr.dev/inspr/pkg/meta"
)

// AppDI - Data Input(DI) format for requests that pass the app data
type AppDI struct {
	App    meta.App `json:"app"`
	Ctx    string   `json:"ctx"`
	DryRun bool     `json:"dry"`
}

// AppQueryDI - Data Input format for queries requests
type AppQueryDI struct {
	Ctx    string `json:"ctx"`
	DryRun bool   `json:"dry"`
}
