package dictionary

import (
	"errors"
	"fmt"
	"reflect"
)

func MapMerge(desk interface{}, ms ...interface{}) (err error) {
	deskType := reflect.TypeOf(desk)
	if deskType.Kind() != reflect.Ptr {
		err = errors.New("desk type must be Ptr")
		return
	}

	if deskType.Elem().Kind() != reflect.Map {
		err = errors.New("desk value must be Map")
		return
	}

	deskVal := reflect.ValueOf(desk)
	direct := reflect.Indirect(deskVal)
	for i := 0; i < reflect.ValueOf(ms).Len(); i++ {
		currentMs := reflect.ValueOf(ms[i])
		switch currentMs.Kind() {
		case reflect.Map:
			iter := currentMs.MapRange()
			for iter.Next() {
				direct.SetMapIndex(iter.Key(), iter.Value())
			}
		case reflect.Struct:
			for s := 0; s < currentMs.NumField(); s++ {
				direct.SetMapIndex(reflect.ValueOf(reflect.TypeOf(currentMs).Field(s).Name), currentMs.Field(s))
			}
		default:
			err = errors.New(fmt.Sprintf("ms'%d param type err", i))
		}
	}
	return err
}
