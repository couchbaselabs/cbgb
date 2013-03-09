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
	"strconv"

	"github.com/robertkrimen/otto"
)

// Originally from github.com/couchbaselabs/walrus, but capitalized.
// TODO: Push this back to walrus.
func OttoToGoArray(array *otto.Object) ([]interface{}, error) {
	lengthVal, err := array.Get("length")
	if err != nil {
		return nil, err
	}
	length, err := lengthVal.ToInteger()
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, length)
	for i := 0; i < int(length); i++ {
		item, err := array.Get(strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		result[i], err = item.Export()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

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

func arrayPrefix(arrayMaybe interface{}, prefixLen int) []interface{} {
	if prefixLen <= 0 {
		return nil
	}
	switch arrayMaybe.(type) {
	case []interface{}:
		array := arrayMaybe.([]interface{})
		if prefixLen > len(array) {
			prefixLen = len(array)
		}
		return array[0:prefixLen]
	default:
	}
	return nil
}
