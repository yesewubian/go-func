package fun

import (
	"errors"
	"fmt"
	"reflect"
)

func CallUserFunc(funcM map[string]interface{}, funcName string, params ...interface{}) (result []reflect.Value, err error) {
	f := reflect.ValueOf(funcM[funcName])
	if len(params) != f.Type().NumIn() {
		err = errors.New(fmt.Sprintf("the number of params must be %d, given %d", f.Type().NumIn(), len(params)))
		return
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	result = f.Call(in)
	return
}
