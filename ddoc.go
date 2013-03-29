package cbgb

import (
	"fmt"

	"github.com/dustin/gomemcached"
)

type DDoc struct {
	Language string       `json:"language,omitempty"`
	Views    Views        `json:"views,omitempty"`
	Options  *DDocOptions `json:"options,omitempty"`
}

type DDocOptions struct {
	LocalSeq      bool `json:"local_seq,omitempty"`
	IncludeDesign bool `json:"include_design,omitempty"`
}

type Views map[string]View

type View struct {
	Map    string `json:"map"`
	Reduce string `json:"reduce,omitempty"`
}

func (b *livebucket) GetDDocVBucket() *VBucket {
	return b.vbucketDDoc
}

func (b *livebucket) GetDDoc(ddocId string) ([]byte, error) {
	res := b.vbucketDDoc.get([]byte(ddocId))
	if res.Status == gomemcached.KEY_ENOENT {
		return nil, nil
	}
	if res.Status != gomemcached.SUCCESS {
		return nil, fmt.Errorf("no ddoc: %v, status: %v", ddocId, res.Status)
	}
	return res.Body, nil
}

func (b *livebucket) SetDDoc(ddocId string, body []byte) error {
	res := vbMutate(b.vbucketDDoc, nil, &gomemcached.MCRequest{
		Opcode: gomemcached.SET,
		Key:    []byte(ddocId),
		Body:   body,
	})
	if res.Status != gomemcached.SUCCESS {
		return fmt.Errorf("set ddoc failed: %v, status: %v", ddocId, res.Status)
	}
	return nil
}

func (b *livebucket) VisitDDocs(start []byte,
	visitor func(key []byte, data []byte) bool) error {
	return b.vbucketDDoc.Visit(start, visitor)
}
