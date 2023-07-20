package dictionary

import (
	goFuncStr "github.com/yesewubian/go-func/string"
)

func MapKeySnakeToCamel(m map[string]interface{}) map[string]interface{} {
	for k, v := range m {
		delete(m, k)
		m[goFuncStr.StrSnakeToCamel(k)] = v
	}
	return m
}
