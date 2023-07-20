package object

import (
	dict "github.com/yesewubian/go-func/dictionary"
)

func StructMergeToMap(desk interface{}, ms ...interface{}) (err error) {
	return dict.MapMerge(desk, ms...)
}
