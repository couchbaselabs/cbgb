package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestViewQuerySmoke(t *testing.T) {
	d, _, bucket := testSetupDefaultBucket(t, 1, uint16(0))
	defer os.RemoveAll(d)
	mr := testSetupMux(d)

	testSetupDDoc(t, bucket, `{
		"_id":"_design/d0",
		"language": "javascript",
		"views": {
			"v0": {
				"map": "function(doc) { emit(doc.amount, null) }"
			}
		}
    }`, nil)

	rr := httptest.NewRecorder()
	r, _ := http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?startkey=1&endkey=3&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd := &ViewResult{}
	err := json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k := []string{"a", "d", "b"}
	a := []int{1, 2, 3}
	if dd.TotalRows != len(k) {
		t.Fatalf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?startkey=2&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k = []string{"d", "b", "c"}
	a = []int{2, 3, 4}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}
}

func TestCouchViewBasic(t *testing.T) {
	d, _, bucket := testSetupDefaultBucket(t, 1, uint16(0))
	defer os.RemoveAll(d)
	mr := testSetupMux(d)

	testSetupDDoc(t, bucket, `{
		"_id":"_design/d0",
		"language": "javascript",
		"views": {
			"v0": {
				"map": "function(doc) { emit(doc.amount, null) }"
			}
		}
    }`, nil)

	rr := httptest.NewRecorder()
	r, _ := http.NewRequest("GET", "http://127.0.0.1/default/hello", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 404 {
		t.Errorf("expected req to 404, got: %#v, %v",
			rr, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/not-a-design-doc/_view/v0?stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 404 {
		t.Errorf("expected req to 404, got: %#v, %v",
			rr, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/not-a-view?stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 404 {
		t.Errorf("expected req to 404, got: %#v, %v",
			rr, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd := &ViewResult{}
	err := json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, err: %v", err)
	}
	k := []string{"a", "d", "b", "c"}
	a := []int{1, 2, 3, 4}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
		if row.Doc != nil {
			t.Errorf("expected no doc since it's not include_docs, got: %#v", row)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?startkey=2&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k = []string{"d", "b", "c"}
	a = []int{2, 3, 4}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?startkey=2&endkey=3&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k = []string{"d", "b"}
	a = []int{2, 3}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?startkey=1&endkey=3&key=2&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k = []string{"d"}
	a = []int{2}
	if dd.TotalRows != len(k) {
		t.Fatalf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?startkey=3&endkey=1&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	if dd.TotalRows != 0 {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			0, dd.TotalRows, dd, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0"+
			"?startkey=1&endkey=3&inclusive_end=false&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k = []string{"a", "d"}
	a = []int{1, 2}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?startkey=2&endkey=4&limit=1&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k = []string{"d"}
	a = []int{2}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?descending=true&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k = []string{"c", "b", "d", "a"}
	a = []int{4, 3, 2, 1}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?"+
			"startkey=3&descending=true&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k = []string{"b", "d", "a"}
	a = []int{3, 2, 1}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?"+
			"include_docs=true&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k = []string{"a", "d", "b", "c"}
	a = []int{1, 2, 3, 4}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
		if row.Doc == nil {
			t.Errorf("expected include_doc to give a doc, got: %#v", row)
		}
		if int(row.Doc.Json.(map[string]interface{})["amount"].(float64)) != (i + 1) {
			t.Errorf("Expected %v at %v, got %v", i+1, i, row.Doc.Json)
		}
	}
}

func TestCouchViewReduceBasic(t *testing.T) {
	d, _, bucket := testSetupDefaultBucket(t, 1, uint16(0))
	defer os.RemoveAll(d)
	mr := testSetupMux(d)

	testSetupDDoc(t, bucket, `{
		"_id":"_design/d0",
		"language": "javascript",
		"views": {
			"v0": {
				"map": "function(doc) { emit(doc.amount, 1); }",
				"reduce": "function(keys, values, rereduce) {
                              var sum = 0;
                              for (var i = 0; i < values.length; i++) {
                                sum = sum + values[i];
                              }
                              return sum;
                           }"
			}
		}
    }`, nil)

	rr := httptest.NewRecorder()
	r, _ := http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd := &ViewResult{}
	err := json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	if dd.TotalRows != 1 {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			1, dd.TotalRows, dd, rr.Body.String())
	}
	exp := 4
	if exp != int(dd.Rows[0].Value.(float64)) {
		t.Errorf("expected row value %#v to match %#v in row %#v",
			dd.Rows[0].Value, exp, dd.Rows[0])
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?reduce=false&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k := []string{"a", "d", "b", "c"}
	a := []int{1, 2, 3, 4}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?"+
			"reduce=true&startkey=2&endkey=3&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	if dd.TotalRows != 1 {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			1, dd.TotalRows, dd, rr.Body.String())
	}
	exp = 2
	if exp != int(dd.Rows[0].Value.(float64)) {
		t.Errorf("expected row value %#v to match %#v in row %#v",
			dd.Rows[0].Value, exp, dd.Rows[0])
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?"+
			"reduce=true&startkey=2&endkey=3&skip=100&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	if dd.TotalRows != 0 {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			0, dd.TotalRows, dd, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?"+
			"reduce=true&descending=true&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	if dd.TotalRows != 1 {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			1, dd.TotalRows, dd, rr.Body.String())
	}
	exp = 4
	if exp != int(dd.Rows[0].Value.(float64)) {
		t.Errorf("expected row value %#v to match %#v in row %#v",
			dd.Rows[0].Value, exp, dd.Rows[0])
	}
}

func TestCouchViewGroupLevel(t *testing.T) {
	d, _, bucket := testSetupDefaultBucket(t, 1, uint16(0))
	defer os.RemoveAll(d)
	mr := testSetupMux(d)

	testSetupDDoc(t, bucket, `{
		"_id":"_design/d0",
		"language": "javascript",
		"views": {
			"v0": {
				"map": "function(doc) { emit([doc.category, doc.amount], 1); }",
				"reduce": "function(keys, values, rereduce) {
                              var sum = 0;
                              for (var i = 0; i < values.length; i++) {
                                sum = sum + values[i];
                              }
                              return sum;
                           }"
			}
		}
    }`, func(i int) string {
		return fmt.Sprintf(`{"amount":%d,"category":%d}`, i, i/2)
	})

	rr := httptest.NewRecorder()
	r, _ := http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd := &ViewResult{}
	err := json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	if dd.TotalRows != 1 {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			1, dd.TotalRows, dd, rr.Body.String())
	}
	exp := 4
	if exp != int(dd.Rows[0].Value.(float64)) {
		t.Errorf("expected row value %#v to match %#v in row %#v",
			dd.Rows[0].Value, exp, dd.Rows[0])
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?"+
			"reduce=false&limit=1000&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	k := []string{"a", "d", "b", "c"}
	a := []string{"[0,1]", "[1,2]", "[1,3]", "[2,4]"}
	if dd.TotalRows != len(k) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(k), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		j, _ := json.Marshal(row.Key)
		if a[i] != string(j) {
			t.Errorf("expected row %#v to match a %#v, i %v, j %v",
				row, a[i], i, j)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?"+
			"group_level=1&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	g := []string{"[0]", "[1]", "[2]"}
	v := []int{1, 2, 1}
	if dd.TotalRows != len(g) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(g), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		j, _ := json.Marshal(row.Key)
		if g[i] != string(j) {
			t.Errorf("expected row %#v to match key %#v, i %v, j %v",
				row, a[i], i, j)
		}
		if v[i] != int(row.Value.(float64)) {
			t.Errorf("expected row %#v to match val %#v, i %v",
				row, v[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?"+
			"group_level=2&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	g = []string{"[0,1]", "[1,2]", "[1,3]", "[2,4]"}
	v = []int{1, 1, 1, 1}
	if dd.TotalRows != len(g) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(g), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		j, _ := json.Marshal(row.Key)
		if g[i] != string(j) {
			t.Errorf("expected row %#v to match key %#v, i %v, j %v",
				row, a[i], i, j)
		}
		if v[i] != int(row.Value.(float64)) {
			t.Errorf("expected row %#v to match val %#v, i %v",
				row, v[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?"+
			"group_level=2&skip=1&limit=2&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	g = []string{"[1,2]", "[1,3]"}
	v = []int{1, 1}
	if dd.TotalRows != len(g) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(g), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		j, _ := json.Marshal(row.Key)
		if g[i] != string(j) {
			t.Errorf("expected row %#v to match key %#v, i %v, j %v",
				row, a[i], i, j)
		}
		if v[i] != int(row.Value.(float64)) {
			t.Errorf("expected row %#v to match val %#v, i %v",
				row, v[i], i)
		}
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0?"+
			"group=true&stale=false", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd = &ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	g = []string{"[0,1]", "[1,2]", "[1,3]", "[2,4]"}
	v = []int{1, 1, 1, 1}
	if dd.TotalRows != len(g) {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			len(g), dd.TotalRows, dd, rr.Body.String())
	}
	for i, row := range dd.Rows {
		j, _ := json.Marshal(row.Key)
		if g[i] != string(j) {
			t.Errorf("expected row %#v to match key %#v, i %v, j %v",
				row, a[i], i, j)
		}
		if v[i] != int(row.Value.(float64)) {
			t.Errorf("expected row %#v to match val %#v, i %v",
				row, v[i], i)
		}
	}
}

func TestReverseViewRows(t *testing.T) {
	var r ViewRows
	r = []*ViewRow{}
	reverseViewRows(r)
	if len(r) != 0 {
		t.Errorf("reversing empty ViewRows should work, got: %#v", r)
	}

	r = []*ViewRow{
		&ViewRow{Id: "a"},
	}
	reverseViewRows(r)
	if len(r) != 1 {
		t.Errorf("reversing empty ViewRows should work, got: %#v", r)
	}
	if r[0].Id != "a" {
		t.Errorf("reversing empty ViewRows should work, got: %#v", r)
	}

	r = []*ViewRow{
		&ViewRow{Id: "a"},
		&ViewRow{Id: "b"},
	}
	reverseViewRows(r)
	if len(r) != 2 {
		t.Errorf("reversing empty ViewRows should work, got: %#v", r)
	}
	if r[0].Id != "b" || r[1].Id != "a" {
		t.Errorf("reversing empty ViewRows should work, got: %#v", r)
	}

	r = []*ViewRow{
		&ViewRow{Id: "a"},
		&ViewRow{Id: "b"},
		&ViewRow{Id: "c"},
	}
	reverseViewRows(r)
	if len(r) != 3 {
		t.Errorf("reversing empty ViewRows should work, got: %#v", r)
	}
	if r[0].Id != "c" || r[1].Id != "b" || r[2].Id != "a" {
		t.Errorf("reversing empty ViewRows should work, got: %#v", r)
	}

	r = []*ViewRow{
		&ViewRow{Id: "a"},
		&ViewRow{Id: "b"},
		&ViewRow{Id: "c"},
		&ViewRow{Id: "d"},
	}
	reverseViewRows(r)
	if len(r) != 4 {
		t.Errorf("reversing empty ViewRows should work, got: %#v", r)
	}
	if r[0].Id != "d" || r[1].Id != "c" || r[2].Id != "b" || r[3].Id != "a" {
		t.Errorf("reversing empty ViewRows should work, got: %#v", r)
	}
}
