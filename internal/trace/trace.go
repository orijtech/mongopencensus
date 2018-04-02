// Copyright (C) MongoDB, Inc. 2018-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package trace

import (
	"context"
	"runtime"
	"strings"
	"sync"

	"go.opencensus.io/trace"
)

// The purpose of baseCommonName is to help trim out the Longest Common Prefix
// so that different spans will have disambiguated names e.g.
//    mongo.(*Collection).InsertOne
// instead of:
//    github.com/mongodb/mongo-go-driver/mongo.(*Collection).InsertOne
// in a non-invasive way.
func caller0() string {
	pc, _, _, _ := runtime.Caller(0)
	fn := runtime.FuncForPC(pc)
	return fn.Name()
}

var baseCommonPath = caller0()

var lcpCacheMtx sync.Mutex
var lcpCache = make(map[string]string)

func longestCommonPrefix(p1, p2 string) string {
	// In the common case we'll have the same base path hence
	// we should start checking from the end
	min, max := p1, p2
	if len(max) < len(min) {
		min, max = max, min
	}
	for i := 0; i < len(min); i++ {
		if max[i] != min[i] {
			return min[:i]
		}
	}
	return min
}

func SpanFromFunctionCaller(ctx context.Context) (context.Context, *trace.Span) {
	// The call to relativeName is an extra
	// function call away from the original.
	nCallers := 2
	return trace.StartSpan(ctx, relativeName(nCallers))
}

func SpanWithName(ctx context.Context, name string) (context.Context, *trace.Span) {
	return trace.StartSpan(ctx, name)
}

func RelativeName() string {
	return relativeName(2)
}

func relativeName(nCaller int) string {
	pc, _, _, _ := runtime.Caller(nCaller)
	fn := runtime.FuncForPC(pc)
	fnName := fn.Name()
	lcpName, ok := lcpCache[fnName]

	if !ok {
		lcpCacheMtx.Lock()
		disambiguatedPrefix := longestCommonPrefix(baseCommonPath, fnName)
		trimmed := strings.TrimPrefix(fnName, disambiguatedPrefix)
		lcpCache[fnName] = trimmed
		lcpName = trimmed
		lcpCacheMtx.Unlock()
	}

	return lcpName
}
