package encrypt

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

//替换字符集
const repCharSet string = "sAwxtyzvBu"

//随机字符集
const charSet string = "abcdefghijklmnopqrCDEFGHIJKLMNOPQRSTUVWXYZ"

// NumberToString 将数字转换成随机字母
func NumberToString(num, numLen int) (string, error) {
	if numLen > 42 {
		return "", fmt.Errorf("the value of numLen needs to be less than 43, %d given", numLen)
	}
	// 我们可以使用随机数生成器来生成随机字符
	rand.Seed(time.Now().UnixNano() + int64(num))
	// 将数字转换为字符串，并在前面补零，使其长度为 12
	numStr := strconv.Itoa(num)
	result := make([]byte, numLen)
	// copy(result, numStr)
	for i := 0; i < len(numStr); i++ {
		result[i] = repCharSet[i]
	}
	// 使用随机数生成器来填充剩余的字符
	for i := len(numStr); i < numLen; i++ {
		result[i] = charSet[rand.Intn(len(charSet))]
	}
	//打乱顺序
	rand.Shuffle(len(result), func(i, j int) {
		result[i], result[j] = result[j], result[i]
	})
	return string(result), nil
}
