//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"encoding/json"
	"fmt"

	"github.com/robertkrimen/otto"
)

func OttoFromGoArray(o *otto.Otto, arr []interface{}) (otto.Value, error) {
	ovarr, err := OttoFromGo(o, arr)
	if err != nil {
		return otto.UndefinedValue(),
			fmt.Errorf("could not convert arr, err: %v", err)
	}
	if ovarr.Class() != "Array" {
		return otto.UndefinedValue(),
			fmt.Errorf("expected ovarr to be array, got: %#v, class: %v, arr: %v",
				ovarr, ovarr.Class(), arr)
	}
	return ovarr, nil
}

func OttoFromGo(o *otto.Otto, v interface{}) (otto.Value, error) {
	jv, err := json.Marshal(v)
	if err != nil {
		return otto.UndefinedValue(),
			fmt.Errorf("could not jsonify v, err: %v", err)
	}
	obj, err := o.Object("({v:" + string(jv) + "})")
	if err != nil {
		return otto.UndefinedValue(),
			fmt.Errorf("could not convert jv, err: %v", err)
	}
	objv, err := obj.Get("v")
	if err != nil {
		return otto.UndefinedValue(),
			fmt.Errorf("could not convert obj, err: %v", err)
	}
	return objv, nil
}

func OttoNewFunction(o *otto.Otto, f string) (otto.Value, error) {
	fn, err := o.Object("(" + f + ")")
	if err != nil {
		return otto.UndefinedValue(),
			fmt.Errorf("could not eval function, err: %v", err)
	}
	if fn.Class() != "Function" {
		return otto.UndefinedValue(),
			fmt.Errorf("fn not a function, was: %v", fn.Class())
	}
	fnv := fn.Value()
	if fnv.Class() != "Function" {
		return otto.UndefinedValue(),
			fmt.Errorf("fnv not a function, was: %v", fnv.Class())
	}
	return fnv, nil
}

func ArrayPrefix(arrayMaybe interface{}, prefixLen int) interface{} {
	if prefixLen <= 0 {
		return nil
	}
	switch x := arrayMaybe.(type) {
	case []interface{}:
		if prefixLen > len(x) {
			prefixLen = len(x)
		}
		return x[0:prefixLen]
	default:
	}
	if prefixLen == 1 {
		return arrayMaybe
	}
	return nil
}

func ottoMust(v otto.Value, err error) otto.Value {
	must(err)
	return v
}
