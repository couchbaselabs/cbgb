package cbgb

import (
	"github.com/couchbaselabs/walrus"
)

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
