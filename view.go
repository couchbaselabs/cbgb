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
	"reflect"
	"strconv"
	"strings"

	"github.com/couchbaselabs/walrus"
	"github.com/robertkrimen/otto"
)

type Views map[string]*View

type View struct {
	Map    string `json:"map"`
	Reduce string `json:"reduce,omitempty"`

	preparedViewMapFunction *ViewMapFunction
}

type ViewMapFunction struct {
	otto         *otto.Otto
	mapf         otto.Value
	restartEmits func() (resEmits []*ViewRow, resEmitErr error)
}

// Originally from github.com/couchbaselabs/walrus, but using
// pointers to structs instead of just structs.

type ViewResult struct {
	TotalRows int      `json:"total_rows"`
	Rows      ViewRows `json:"rows"`
}

type ViewRows []*ViewRow

type ViewDocValue struct {
	Meta map[string]interface{} `json:"meta"`
	Json interface{}            `json:"json"`
}

type ViewRow struct {
	Id    string        `json:"id,omitempty"`
	Key   interface{}   `json:"key,omitempty"`
	Value interface{}   `json:"value,omitempty"`
	Doc   *ViewDocValue `json:"doc,omitempty"`
}

func (rows ViewRows) Len() int {
	return len(rows)
}

func (rows ViewRows) Swap(i, j int) {
	rows[i], rows[j] = rows[j], rows[i]
}

func (rows ViewRows) Less(i, j int) bool {
	return walrus.CollateJSON(rows[i].Key, rows[j].Key) < 0
}

// From http://wiki.apache.org/couchdb/HTTP_view_API
type ViewParams struct {
	Key           interface{} `json:"key"`
	Keys          string      `json:"keys"` // TODO: should be []interface{}.
	StartKey      interface{} `json:"startkey" alias:"start_key"`
	StartKeyDocId string      `json:"startkey_docid"`
	EndKey        interface{} `json:"endkey" alias:"end_key"`
	EndKeyDocId   string      `json:"endkey_docid"`
	Stale         string      `json:"stale"`
	Descending    bool        `json:"descending"`
	Group         bool        `json:"group"`
	GroupLevel    uint64      `json:"group_level"`
	IncludeDocs   bool        `json:"include_docs"`
	InclusiveEnd  bool        `json:"inclusive_end"`
	Limit         uint64      `json:"limit"`
	Reduce        bool        `json:"reduce"`
	Skip          uint64      `json:"skip"`
	UpdateSeq     bool        `json:"update_seq"`
}

func NewViewParams() *ViewParams {
	return &ViewParams{
		Reduce:       true,
		InclusiveEnd: true,
	}
}

func paramFieldNames(sf reflect.StructField) []string {
	fieldName := sf.Tag.Get("json")
	if fieldName == "" {
		fieldName = sf.Name
	}
	rv := []string{fieldName}
	alias := sf.Tag.Get("alias")
	if alias != "" {
		rv = append(rv, strings.Split(alias, ",")...)
	}
	return rv
}

func ParseViewParams(params Form) (p *ViewParams, err error) {
	p = NewViewParams()
	if params == nil {
		return p, nil
	}

	val := reflect.Indirect(reflect.ValueOf(p))

	for i := 0; i < val.NumField(); i++ {
		sf := val.Type().Field(i)
		var paramVal string
		for _, n := range paramFieldNames(sf) {
			paramVal = params.FormValue(n)
			if paramVal != "" {
				break
			}
		}

		switch {
		case paramVal == "":
			// Skip this one
		case sf.Type.Kind() == reflect.String:
			val.Field(i).SetString(paramVal)
		case sf.Type.Kind() == reflect.Uint64:
			v := uint64(0)
			v, err = strconv.ParseUint(paramVal, 10, 64)
			if err != nil {
				return nil, err
			}
			val.Field(i).SetUint(v)
		case sf.Type.Kind() == reflect.Bool:
			val.Field(i).SetBool(paramVal == "true")
		case sf.Type.Kind() == reflect.Interface:
			var ob interface{}
			err := json.Unmarshal([]byte(paramVal), &ob)
			if err != nil {
				return p, err
			}
			val.Field(i).Set(reflect.ValueOf(ob))
		default:
			return nil, fmt.Errorf("Unhandled type in field %v", sf.Name)
		}
	}

	return p, nil
}

// Merge incoming, sorted ViewRows by Key.
func MergeViewRows(inSorted []chan *ViewRow, out chan *ViewRow) {
	end := &ViewRow{} // Sentinel.
	arr := make([]*ViewRow, len(inSorted))

	receiveViewRow := func(i int, in chan *ViewRow) {
		v, ok := <-in
		if !ok {
			arr[i] = end
		} else {
			arr[i] = v
		}
	}

	for i, in := range inSorted { // Initialize the arr.
		receiveViewRow(i, in)
	}

	pickLeast := func() (int, *ViewRow) {
		// TODO: Inefficient to iterate over array every time.
		// [probably more inefficient to have this be a
		// closure, but should measure to be certain]
		ileast := -1
		vleast := end
		for i, v := range arr {
			if vleast == end {
				ileast = i
				vleast = v
			} else if v != end {
				if walrus.CollateJSON(vleast.Key, v.Key) > 0 {
					ileast = i
					vleast = v
				}
			}
		}
		return ileast, vleast
	}

	for {
		i, v := pickLeast()
		if v == end {
			close(out)
			return
		}
		out <- v
		receiveViewRow(i, inSorted[i])
	}
}

func (v *View) GetViewMapFunction() (*ViewMapFunction, error) {
	if v.preparedViewMapFunction != nil {
		return v.preparedViewMapFunction, nil
	}
	vmf, err := v.PrepareViewMapFunction()
	if err != nil {
		return nil, err
	}
	v.preparedViewMapFunction = vmf
	return vmf, err
}

func (v *View) PrepareViewMapFunction() (*ViewMapFunction, error) {
	if v.Map == "" {
		return nil, fmt.Errorf("view map function missing")
	}

	o := otto.New()
	mapf, err := OttoNewFunction(o, v.Map)
	if err != nil {
		return nil, fmt.Errorf("view map function error: %v", err)
	}

	emits := []*ViewRow{}
	var emitErr error

	o.Set("emit", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) <= 0 {
			emitErr = fmt.Errorf("emit() invoked with no parameters")
			return otto.UndefinedValue()
		}
		var key, value interface{}
		key, emitErr = call.ArgumentList[0].Export()
		if emitErr != nil {
			return otto.UndefinedValue()
		}
		if len(call.ArgumentList) >= 2 {
			value, emitErr = call.ArgumentList[1].Export()
			if emitErr != nil {
				return otto.UndefinedValue()
			}
		}
		emits = append(emits, &ViewRow{Key: key, Value: value})
		return otto.UndefinedValue()
	})

	return &ViewMapFunction{
		otto: o,
		mapf: mapf,
		restartEmits: func() (resEmits []*ViewRow, resEmitErr error) {
			resEmits = emits
			resEmitErr = emitErr
			emits = []*ViewRow{}
			emitErr = nil
			return resEmits, resEmitErr
		},
	}, nil
}
