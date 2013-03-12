#!/bin/sh -e

project=github.com/couchbaselabs/cbgb
top=`go list -f '{{.Dir}}' $project`
version=`git describe`

cd $top
./gen_version.sh

DIST=$top/dist

testpkg() {
    go test $project/...
    go vet $project/...
}

mkversion() {
    echo "{\"version\": \"$version\"}" > $DIST/version.json
}

build() {
    pkg=$project/cbgb
    goflags="-v -tags has_version"

    env GOARCH=386   GOOS=linux CGO_ENABLED=0 go build $goflags -o $DIST/cbgb.lin32 $pkg &
    env GOARCH=arm   GOOS=linux CGO_ENABLED=0 go build $goflags -o $DIST/cbgb.arm $pkg &
    env GOARCH=arm   GOARM=5 GOOS=linux CGO_ENABLED=0 go build $goflags -o $DIST/cbgb.arm5 $pkg &
    env GOARCH=amd64 GOOS=linux CGO_ENABLED=0 go build $goflags -o $DIST/cbgb.lin64 $pkg &
    env GOARCH=amd64 GOOS=freebsd CGO_ENABLED=0 go build $goflags -o $DIST/cbgb.fbsd $pkg &&
    env GOARCH=386   GOOS=windows go build $goflags -o $DIST/cbgb.win32.exe $pkg &
    env GOARCH=amd64 GOOS=windows go build $goflags -o $DIST/cbgb.win64.exe $pkg &
    env GOARCH=amd64 GOOS=darwin go build $goflags -o $DIST/cbgb.mac $pkg &

    wait
}

compress() {
    rm -f $DIST/cbgb.*.gz || true
    for i in $DIST/cbgb.*
    do
        gzip -9v $i &
    done

    wait
}

static() {
    rm $DIST/static.zip
    cd $top/static
    zip -r $DIST/static.zip *
}

upload() {
    cbfsclient ${cbfsserver:-http://cbfs.hq.couchbase.com:8484/} upload \
        -ignore=$DIST/.cbfsclient.ignore -delete -v \
        $DIST/ cbgb/
}

testpkg
mkversion
build
compress
static
upload
