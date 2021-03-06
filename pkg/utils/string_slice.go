// Package utils has a number of useful operations that are used
// in multiple places of the inspr packages, contains operations
// such as:
//
// 	- compare_options: "comparators and evaluator for slices and maps"
// 	- string_slice: "set of operations of custom string slice"
package utils

import (
	"sort"
	"strings"

	kubeCore "k8s.io/api/core/v1"
)

// Index returns the first index of the target string t,
// or -1 if no match is found.
func Index(vs []string, t string) int {
	for i, v := range vs {
		if v == t {
			return i
		}
	}
	return -1
}

// Includes returns true if the target string t is in the slice.
func Includes(vs []string, t string) bool {
	return Index(vs, t) >= 0
}

// Remove return a new slice without any occurrence of the
// target string t
func Remove(vs []string, t string) []string {
	var newSlice []string
	for _, v := range vs {
		if v != t {
			newSlice = append(newSlice, v)
		}
	}
	return newSlice
}

// StringSliceUnion returns the sorted union of the two string slices.
// Remember that union dont have repeated elements
func StringSliceUnion(a, b []string) []string {
	check := make(map[string]bool)

	d := append(a, b...)
	res := make([]string, 0)

	for _, val := range d {
		if !check[val] {
			res = append(res, val)
		}
		check[val] = true
	}

	return res
}

// Map returns a new slice containing the results of applying the function f to each string in the original slice.
func Map(vs []string, f func(string) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

// StringArray is an array of strings with functional and set-like helper methods
type StringArray []string

func (c StringArray) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
func (c StringArray) Len() int {
	return len(c)
}
func (c StringArray) Less(i, j int) bool {
	return c[i] < c[j]
}

//Sorted is a helper method that sorts an String Array
func (c StringArray) Sorted() StringArray {
	n := StringArray{}
	n = append(n, c...)
	sort.Sort(n)
	return n
}

// Map maps a given function into another string array
func (c StringArray) Map(f func(string) string) StringArray {
	return Map(c, f)
}

// Filter creates a new string array without the filtered items
func (c StringArray) Filter(f func(string) bool) StringArray {
	var ret StringArray
	for _, s := range c {
		if f(s) {
			ret = append(ret, s)
		}
	}
	return ret
}

// Union returns the union of a string array with another
func (c StringArray) Union(other StringArray) StringArray {
	return StringSliceUnion(c, other)
}

// Contains returns whether or not an array contains an item
func (c StringArray) Contains(item string) bool {
	return Includes(c, item)
}

//Equal compares two StringArrays and returns true if both have all of the same items
func (c StringArray) Equal(other StringArray) bool {
	if len(c) != len(other) {
		return false
	}
	for _, item := range c {
		if !other.Contains(item) {
			return false
		}
	}
	return true
}

// Join joins a string array with a given separator, returning the string generated
func (c StringArray) Join(sep string) string {
	return strings.Join(c, sep)
}

// EnvironmentMap is a type for environment variables represented as a map
type EnvironmentMap map[string]string

// ParseToK8sArrEnv parses the map into an array of kubernetes' environment variables
func (m EnvironmentMap) ParseToK8sArrEnv() []kubeCore.EnvVar {
	var arrEnv []kubeCore.EnvVar
	for key, val := range m {
		arrEnv = append(arrEnv, kubeCore.EnvVar{
			Name:  key,
			Value: val,
		})
	}
	return arrEnv
}

//ParseFromK8sEnvironment is the oposing function to ParseToK8sArrEnv.
func ParseFromK8sEnvironment(envs []kubeCore.EnvVar) EnvironmentMap {
	nodeEnv := make(map[string]string)
	for _, env := range envs {
		nodeEnv[env.Name] = env.Value
	}
	return nodeEnv
}
