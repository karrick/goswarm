package goswarm

import "fmt"

// Value1 is a type whose semantic value might be unknown, have some proper
// value, or have an error.  It is a hybrid between an Option and a Result in
// Rust.
type Value1 struct {
	value  interface{}
	status VST
}

func (v Value1) Load() (interface{}, error, bool) {
	if v.status == ValueNone {
		return nil, nil, false
	}
	if v.status == ValueError {
		return nil, v.value.(error), true
	}
	return v.value, nil, true
}

func (v *Value1) StoreValue(value interface{}) {
	v.value = value
	v.status = ValueFresh
}

func (v *Value1) StoreError(err error) {
	v.value = err
	v.status = ValueError
}

func (v *Value1) StoreErrorf(format string, a ...interface{}) {
	v.value = fmt.Errorf(format, a...)
	v.status = ValueError
}

// Reset returns the instance to its initialized state.
func (v *Value1) Reset() {
	v.value = nil
	v.status = ValueNone
}
