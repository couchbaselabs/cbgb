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

package cbgb

import (
	"github.com/couchbaselabs/walrus"
)

// Originally from github.com/couchbaselabs/walrus, but using
// pointers to structs instead of just structs.

type ViewResult struct {
	TotalRows int      `json:"total_rows"`
	Rows      ViewRows `json:"rows"`
}

type ViewRows []*ViewRow

type ViewRow struct {
	Id    string      `json:"id,omitempty"`
	Key   interface{} `json:"key,omitempty"`
	Value interface{} `json:"value,omitempty"`
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
