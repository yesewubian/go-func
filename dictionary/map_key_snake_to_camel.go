package dictionary

import (
	str "github.com/yesewubian/go-func/string"
)

func MapKeySnakeToCamel(m map[string]interface{}) map[string]interface{} {
	for k, v := range m {
		delete(m, k)
		m[str.StrSnakeToCamel(k)] = v
	}
	return m
}
