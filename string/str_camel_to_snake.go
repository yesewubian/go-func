package string

import "strings"

func StrCamelToSnake(s string) string {
	if s == "ID" {
		return "id"
	}
	data := make([]byte, 0, len(s)*2)
	j := false
	k := true
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		// or通过ASCII码进行大小写的转化
		// 65-90 (A-Z)，97-122 (a-z)
		//判断如果字母为大写的A-Z就在前面拼接一个
		if i > 0 && d >= 'A' && d <= 'Z' && j && k {
			data = append(data, '_')
			k = false
		} else {
			k = true
		}
		if d != '_' {
			j = true
		}
		data = append(data, d)
	}
	//ToLower把大写字母统一转小写
	return strings.ToLower(string(data[:]))
}
