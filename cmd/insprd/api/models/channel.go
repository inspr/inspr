package models

import "gitlab.inspr.dev/inspr/core/pkg/meta"

// ChannelDI - Data Input format for requests that pass the channel data
type ChannelDI struct {
	Channel meta.Channel `json:"channel"`
	Ctx     string       `json:"ctx"`
	Setup   bool
}

// ChannelQueryDI - Data Input format for queries requests
type ChannelQueryDI struct {
	Ctx    string `json:"ctx"`
	ChName string `json:"chname"`
	Setup  bool
}
