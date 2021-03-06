package utils

import (
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
	"inspr.dev/inspr/pkg/meta"
)

// CompareWithoutUUID compares two components without comparing their UUID.
func CompareWithoutUUID(first, second interface{}) bool {
	opt := cmp.Comparer(func(a, b meta.Metadata) bool {
		a.UUID = ""
		b.UUID = ""
		return cmp.Equal(a, b)
	})
	return cmp.Equal(first, second, opt)
}

// CompareWithUUID compares two components with their UUIDs
func CompareWithUUID(first, second interface{}) bool {

	return cmp.Equal(first, second)
}

var regexUUID = regexp.MustCompile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

// ValidateUUID validates the formatting of a string to check whether the string contains a proper UUID
func ValidateUUID(uuid string) bool {
	return regexUUID.MatchString(uuid)
}

// RecursiveValidateUUIDS validates UUIDs on a structure recursively
func RecursiveValidateUUIDS(name string, app *meta.App, t *testing.T) {
	if !ValidateUUID(app.Meta.UUID) {
		t.Errorf("%s invalid UUID on %s, uuid=%v", name, app.Meta.Name, app.Meta.UUID)
	}
	for _, a := range app.Spec.Apps {

		RecursiveValidateUUIDS(name, a, t)
	}
	for _, c := range app.Spec.Channels {
		if !ValidateUUID(c.Meta.UUID) {
			t.Errorf("%s invalid channel UUID on %s, uuid = %v", name, c.Meta.Name, c.Meta.UUID)
		}
	}

	for _, insprType := range app.Spec.Types {
		if !ValidateUUID(insprType.Meta.UUID) {
			t.Errorf(
				"%s invalid Type UUID on %s, uuid = %v",
				name,
				insprType.Meta.Name,
				insprType.Meta.UUID,
			)
		}
	}

	for _, a := range app.Spec.Aliases {
		if !ValidateUUID(a.Meta.UUID) {
			t.Errorf("%s invalid alias UUID on %s, uuid = %v", name, a.Meta.Name, a.Meta.UUID)
		}
	}

}
