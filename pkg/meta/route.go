package meta

import (
	"inspr.dev/inspr/pkg/utils"
)

// Route is an Inspr component that represents a route.
// Endpoints is a list of allowed endpoints on regarding the route
type Route struct {
	Meta      Metadata          `yaml:"meta,omitempty" json:"meta"`
	Endpoints utils.StringArray `yaml:"endpoints,omitempty"  json:"endpoints"`
}

// RouteConnection is the structure to the pod address and its endpoints
type RouteConnection struct {
	Address   string
	Endpoints utils.StringArray
}
