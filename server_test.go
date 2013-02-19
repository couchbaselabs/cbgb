package cbgb

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/dustin/gomemcached"
)

func TestSaslListMechs(t *testing.T) {
	rh := reqHandler{nil}
	res := rh.HandleMessage(ioutil.Discard, &gomemcached.MCRequest{
		Opcode: gomemcached.SASL_LIST_MECHS,
	})
	if res == nil {
		t.Errorf("expected SASL_LIST_MECHS to be non-nil")
	}
	if !bytes.Equal(res.Body, []byte("PLAIN")) {
		t.Errorf("expected SASL_LIST_MECHS to be PLAIN")
	}
}
