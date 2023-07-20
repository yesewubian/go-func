package slice

import (
	"errors"
	"fmt"
	"reflect"
)

/**
 * indexKey == ""  && columnKey != "" 将切片类型的Map，转成 slice 输出，输出结果存在了 desk 中
 * indexKey != ""  && columnKey == "" 将切片类型的Map，转成 map 输出，输出结果存在了 desk 中
 * indexKey != ""  && columnKey != "" 将切片类型的Map，转成 map 输出，输出结果存在了 desk 中
 *
 * 假如有下述切片类型 ([]map[string]interface{}) 的变量 input，
 * [
 *  {"id":1,"name":"zhang"},
 *  {"id":2,"name":"li"},
 * ]
 *
 * @demo 1
 * 输入：
 * @params desk &[]int32
 * @params input
 * @params columnKey 列名 "id"
 * @params indexKey 键名 ""
 *
 * 输出：
 * err 错误信息
 * 入参 desk 已经被赋值：[]int32{1, 2}
 *
 *
 * @demo 2
 * 输入：
 * @params desk &map[int32]map[string]interface{}
 * @params input
 * @params columnKey 列名 ""
 * @params indexKey 键名 "id"
 *
 * 输出：
 * err 错误信息
 * 入参 desk 已经被赋值：map[ 1:map{"id":1,"name":"zhang"}, 2:{"id":2,"name":"li"} ]
 *
 *
 * @demo 3
 * 输入：
 * @params desk &map[int32]interface{}
 * @params input
 * @params columnKey 列名 "name"
 * @params indexKey 键名 "id"
 *
 * 输出：
 * err 错误信息
 * 入参 desk 已经被赋值：map[ 1:"zhang", 2:"li" ]
 */
func SliceColumn(desk, input interface{}, columnKey, indexKey string) (err error) {
	structIndexColumn := func(desk, input interface{}, columnKey, indexKey string) (err error) {
		findStructValByIndexKey := func(curVal reflect.Value, elemType reflect.Type, indexKey, columnKey string) (indexVal, columnVal reflect.Value, err error) {
			index := reflect.ValueOf(indexKey)
			indexVal = curVal.MapIndex(index)

			if columnKey != "" {
				column := reflect.ValueOf(columnKey)
				columnVal = curVal.MapIndex(column)
			} else {
				columnVal = curVal
			}
			return
		}

		deskValue := reflect.ValueOf(desk)
		if deskValue.Elem().Kind() != reflect.Map {
			return errors.New("desk must be map")
		}
		deskElem := deskValue.Type().Elem()

		rv := reflect.ValueOf(input)
		rt := reflect.TypeOf(input)
		elemType := rt.Elem()

		var indexVal, columnVal reflect.Value
		direct := reflect.Indirect(deskValue)
		mapReflect := reflect.MakeMap(deskElem)
		deskKey := deskValue.Type().Elem().Key()

		for i := 0; i < rv.Len(); i++ {
			curVal := rv.Index(i)
			indexVal, columnVal, err = findStructValByIndexKey(curVal, elemType, indexKey, columnKey)
			if err != nil {
				return
			}
			if deskKey.Kind() != indexVal.Kind() {
				return errors.New(fmt.Sprintf("cant't convert %s to %s, your map'key must be %s", indexVal.Kind(), deskKey.Kind(), indexVal.Kind()))
			}
			if len(columnKey) == 0 {
				mapReflect.SetMapIndex(indexVal, curVal)
				direct.Set(mapReflect)
			} else {
				if deskElem.Elem().Kind() != columnVal.Kind() {
					return errors.New(fmt.Sprintf("your map must be map[%s]%s", indexVal.Kind(), columnVal.Kind()))
				}
				mapReflect.SetMapIndex(indexVal, columnVal)
				direct.Set(mapReflect)
			}
		}
		return
	}

	structColumn := func(desk, input interface{}, columnKey string) (err error) {
		findStructValByColumnKey := func(curVal reflect.Value, elemType reflect.Type, columnKey string) (columnVal reflect.Value, err error) {
			column := reflect.ValueOf(columnKey)
			columnVal = curVal.MapIndex(column)
			return
		}

		if len(columnKey) == 0 {
			return errors.New("columnKey cannot not be empty")
		}

		deskElemType := reflect.TypeOf(desk).Elem()
		if deskElemType.Kind() != reflect.Slice {
			return errors.New("desk must be slice")
		}

		rv := reflect.ValueOf(input)
		rt := reflect.TypeOf(input)

		var columnVal reflect.Value
		deskValue := reflect.ValueOf(desk)
		direct := reflect.Indirect(deskValue)

		for i := 0; i < rv.Len(); i++ {
			columnVal, err = findStructValByColumnKey(rv.Index(i), rt.Elem(), columnKey)
			if err != nil {
				return
			}
			if deskElemType.Elem().Kind() != columnVal.Kind() {
				return errors.New(fmt.Sprintf("your slice must be []%s", columnVal.Kind()))
			}

			direct.Set(reflect.Append(direct, columnVal))
		}
		return
	}

	deskValue := reflect.ValueOf(desk)
	if deskValue.Kind() != reflect.Ptr {
		return errors.New("desk must be ptr")
	}

	rv := reflect.ValueOf(input)
	if rv.Kind() != reflect.Slice {
		return errors.New("input must be slice")
	}

	rt := reflect.TypeOf(input)
	if rt.Elem().Kind() != reflect.Map {
		return errors.New("input's elem must be map")
	}

	if len(indexKey) > 0 {
		return structIndexColumn(desk, input, columnKey, indexKey)
	}
	return structColumn(desk, input, columnKey)
}
