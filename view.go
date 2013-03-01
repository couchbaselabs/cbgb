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
	"strconv"

	"github.com/couchbaselabs/walrus"
)

type Form interface {
	FormValue(key string) string
}

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

// From http://wiki.apache.org/couchdb/HTTP_view_API
type ViewParams struct {
	Key           string `json:"key"`
	Keys          string `json:"keys"`
	StartKey      string `json:"startkey"`
	StartKeyDocId string `json:"startkey_docid"`
	EndKey        string `json:"endkey"`
	EndKeyDocId   string `json:"endkey_docid"`
	Stale         string `json:"stale"`
	Descending    bool   `json:"descending"`
	Group         bool   `json:"group"`
	GroupLevel    uint64 `json:"group_level"`
	IncludeDocs   bool   `json:"include_docs"`
	InclusiveEnd  bool   `json:"inclusive_end"`
	Limit         uint64 `json:"limit"`
	Reduce        bool   `json:"reduce"`
	Skip          uint64 `json:"skip"`
	UpdateSeq     bool   `json:"update_seq"`
}

func NewViewParams() *ViewParams {
	return &ViewParams{
		Reduce:       true,
		InclusiveEnd: true,
	}
}
func ParseViewParams(params Form) (p *ViewParams, err error) {
	p = NewViewParams()
	if params == nil {
		return p, nil
	}
	p.Key = params.FormValue("key")
	p.Keys = params.FormValue("keys")
	p.StartKey = params.FormValue("startkey")
	p.StartKeyDocId = params.FormValue("startkey_docid")
	p.EndKey = params.FormValue("endkey")
	p.EndKeyDocId = params.FormValue("endkey_docid")
	p.Stale = params.FormValue("stale")

	var bv bool
	var iv uint64
	var s string

	if s = params.FormValue("descending"); len(s) > 0 {
		if bv, err = strconv.ParseBool(s); err != nil {
			return p, err
		}
		p.Descending = bv
	}
	if s = params.FormValue("group"); len(s) > 0 {
		if bv, err = strconv.ParseBool(s); err != nil {
			return p, err
		}
		p.Group = bv
	}
	if s = params.FormValue("group_level"); len(s) > 0 {
		if iv, err = strconv.ParseUint(s, 10, 64); err != nil {
			return p, err
		}
		p.GroupLevel = iv
	}
	if s = params.FormValue("include_docs"); len(s) > 0 {
		if bv, err = strconv.ParseBool(s); err != nil {
			return p, err
		}
		p.IncludeDocs = bv
	}
	if s = params.FormValue("inclusive_end"); len(s) > 0 {
		if bv, err = strconv.ParseBool(s); err != nil {
			return p, err
		}
		p.InclusiveEnd = bv
	}
	if s = params.FormValue("limit"); len(s) > 0 {
		if iv, err = strconv.ParseUint(s, 10, 64); err != nil {
			return p, err
		}
		p.Limit = iv
	}
	if s = params.FormValue("reduce"); len(s) > 0 {
		if bv, err = strconv.ParseBool(s); err != nil {
			return p, err
		}
		p.Reduce = bv
	}
	if s = params.FormValue("skip"); len(s) > 0 {
		if iv, err = strconv.ParseUint(s, 10, 64); err != nil {
			return p, err
		}
		p.Skip = iv
	}
	if s = params.FormValue("update_seq"); len(s) > 0 {
		if bv, err = strconv.ParseBool(s); err != nil {
			return p, err
		}
		p.UpdateSeq = bv
	}

	return p, nil
}
