package test

// NopReporter is a no-op kube.Reporter.
type NopReporter struct{}

func (NopReporter) Info(_ string, _ ...interface{})           {}
func (NopReporter) Debug(_ string, _ ...interface{})          {}
func (NopReporter) Error(_ error, _ string, _ ...interface{}) {}
func (NopReporter) RecordInfo(_, _ string)                    {}
func (NopReporter) RecordError(_ string, _ error)             {}
