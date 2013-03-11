#!/bin/sh

sed s/@VERSION@/`git describe --always`/ > version_git.go <<EOF
// +build has_version

package cbgb

const VERSION="@VERSION@"
EOF
