package dictionary

import "reflect"

func MapFilterZero(desk interface{}) {
	deskVal := reflect.ValueOf(desk).Elem()
	direct := reflect.Indirect(deskVal)
	iter := deskVal.MapRange()
	for iter.Next() {
		switch iter.Value().Kind() {
		case reflect.String:
			if iter.Value().String() == "" {
				direct.SetMapIndex(iter.Key(), reflect.ValueOf(""))
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if iter.Value().Int() == 0 {
				direct.SetMapIndex(iter.Key(), reflect.ValueOf(0))
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if iter.Value().Uint() == 0 {
				direct.SetMapIndex(iter.Key(), reflect.ValueOf(0))
			}
		case reflect.Float32, reflect.Float64:
			if iter.Value().Float() == 0 {
				direct.SetMapIndex(iter.Key(), reflect.ValueOf(0))
			}
		default:
			panic("unexpectedly type of desk's elem")
		}
	}
}
