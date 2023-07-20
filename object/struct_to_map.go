package object

import (
	"errors"
	"reflect"
)

// 将结构体转为map,默认用tag.json做为key,没有则用字段名

// type User struct {
// 	Name      string `json:"name"`
// 	Age       int
// 	CreatedAt int `json:"created_at"`
// }
// var user User
// user.Name = "li"
// user.Age = 1
// user.CreatedAt = 100
// var userMap = make(map[string]interface{}, 3)
// MapToStruct(&userMap, &user)
// fmt.Println("u:", userMap)

// u: map[Age:1 created_at:100 name:li]
func StructToMap(desk interface{}, data interface{}) (err error) {
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

	dataValue := reflect.ValueOf(data).Elem()
	dataType := reflect.TypeOf(data).Elem()

	for s := 0; s < dataValue.NumField(); s++ {
		tagJson := dataType.Field(s).Tag.Get("json")
		if tagJson != "" {
			direct.SetMapIndex(reflect.ValueOf(dataType.Field(s).Tag.Get("json")), dataValue.Field(s))
		} else {
			direct.SetMapIndex(reflect.ValueOf(dataType.Field(s).Name), dataValue.Field(s))
		}
	}
	return
}
