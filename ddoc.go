package cbgb

import (
	"fmt"

	"github.com/dustin/gomemcached"
)

func (b *livebucket) GetDDocVBucket() *vbucket {
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
