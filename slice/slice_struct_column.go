package slice

import (
	"errors"
	"fmt"
	"reflect"
)

/**
 * len(indexKey) > 0 将切片类型的结构体，转成 map 输出，输出结果存在了 desk 中，也就是你的第一个参数
 * len(indexKey) == 0 将切片类型的结构体，转成 slice 输出，输出结果存在了 desk 中，也就是你的第一个参数
 *
 * @demo 假如有下述结构体，
 * type User struct {
 *  ID   int
 *  NAME string
 * }
 * 输入：
 * @params desk &map[int64]User   desk是个指针呦！！！
 * @params input []User{User{ID:1, NAME:"zwk"}, User{ID:2, NAME:"zzz"}}
 * @params indexKey 键名 "ID"
 * @params columnKey 列名 空字符串代表返回整个结构体，反之返回结构体中的某一列
 *
 * 输出：
 * err 错误信息
 * 入参 desk 已经被赋值：map[int]User{1:User{ID:1, NAME:"zwk"}, 2:User{ID:2, NAME:"zzz"}}
 *
 * 输入：
 * @params desk &[]int   desk是个指针呦！！！
 * @params input []User{User{ID:1, NAME:"zwk"}, User{ID:2, NAME:"zzz"}}
 * @params indexKey 键名 ""
 * @params columnKey 列名
 *
 * 输出：
 * err 错误信息
 * 入参 desk 已经被赋值：[]int{1, 2}
 */
func SliceStructColumn(desk, input interface{}, columnKey, indexKey string) (err error) {
	structIndexColumn := func(desk, input interface{}, columnKey, indexKey string) (err error) {
		findStructValByIndexKey := func(curVal reflect.Value, elemType reflect.Type, indexKey, columnKey string) (indexVal, columnVal reflect.Value, err error) {
			indexExist := false
			columnExist := false
			for i := 0; i < elemType.NumField(); i++ {
				curField := curVal.Field(i)
				if elemType.Field(i).Name == indexKey {
					switch curField.Kind() {
					case reflect.String, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int, reflect.Float64, reflect.Float32:
						indexExist = true
						indexVal = curField
					case reflect.Ptr:
						indexExist = true
						indexVal = curField.Elem()
					default:
						return indexVal, columnVal, errors.New("indexKey must be int float or string")
					}
				}
				if elemType.Field(i).Name == columnKey {
					columnExist = true
					columnVal = curField
					continue
				}
			}
			if !indexExist {
				return indexVal, columnVal, errors.New(fmt.Sprintf("indexKey %s not found in %s's field", indexKey, elemType))
			}
			if len(columnKey) > 0 && !columnExist {
				return indexVal, columnVal, errors.New(fmt.Sprintf("columnKey %s not found in %s's field", columnKey, elemType))
			}
			return
		}

		deskValue := reflect.ValueOf(desk)
		if deskValue.Elem().Kind() != reflect.Map {
			return errors.New("desk must be map")
		}
		deskElem := deskValue.Type().Elem()
		if len(columnKey) == 0 && deskElem.Elem().Kind() != reflect.Struct {
			return errors.New(fmt.Sprintf("desk's elem expect struct, got %s", deskElem.Elem().Kind()))
		}

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
			columnExist := false
			for i := 0; i < elemType.NumField(); i++ {
				curField := curVal.Field(i)
				if elemType.Field(i).Name == columnKey {
					columnExist = true
					columnVal = curField
					continue
				}
			}
			if !columnExist {
				return columnVal, errors.New(fmt.Sprintf("columnKey %s not found in %s's field", columnKey, elemType))
			}
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
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return errors.New("input must be map slice or array")
	}

	rt := reflect.TypeOf(input)
	if rt.Elem().Kind() != reflect.Struct {
		return errors.New("input's elem must be struct")
	}

	if len(indexKey) > 0 {
		return structIndexColumn(desk, input, columnKey, indexKey)
	}
	return structColumn(desk, input, columnKey)
}
