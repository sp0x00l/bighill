//go:build !cgo

package metrics

func ClassifyKafka(_ error) ErrorClass {
	return ErrorClassUnknown
}
