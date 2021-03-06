package meta

import "inspr.dev/inspr/pkg/utils"

// Channel is an Inspr component that represents a Channel.
type Channel struct {
	Meta          Metadata    `yaml:"meta,omitempty"  json:"meta"`
	Spec          ChannelSpec `yaml:"spec,omitempty"  json:"spec"`
	ConnectedApps utils.StringArray
}

// ChannelSpec is the specification of a channel.
// 'Type' string references a Type structure name
type ChannelSpec struct {
	Type               string   `yaml:"type,omitempty"  json:"type" `
	BrokerPriorityList []string `yaml:"brokerlist,omitempty" json:"brokerlist"`
	SelectedBroker     string   `yaml:"selectedbroker,omitempty" json:"selectedbroker"`
}
