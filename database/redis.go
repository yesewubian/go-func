package database

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/ugorji/go/codec"
	"log"
	"reflect"
	"strconv"
	"time"
)

type Redis struct {
	Pool *redis.Pool
}

func (r *Redis) GetPoll() redis.Conn {
	return r.Pool.Get()
}

func (r *Redis) Stats() redis.PoolStats {
	return r.Pool.Stats()
}

func (r *Redis) Get(key string, v interface{}) (err error) {
	var data interface{}
	var conn redis.Conn
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = r.Do(conn, "GET", key)
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	if err != nil {
		return err
	}
	if data != nil {
		return Decode(data, v)
	}
	return nil
}

//分布式锁使用
func (r *Redis) SetNxNew(key string, value interface{}, expire int64) (ok bool, err error) {

	var conn redis.Conn
	var replay interface{}
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		replay, err = r.Do(conn, "SET", key, value, "EX", expire, "NX")
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	switch fmt.Sprintf("%v", replay) {
	case "OK":
		ok = true
	case "<nil>":
		ok = false
	}
	return
}

func (r *Redis) SetNx(key string, expire int64) (ok bool, err error) {

	var conn redis.Conn
	t := time.Now().Unix()
	value := t + expire + 1
	var replay interface{}
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		replay, err = r.Do(conn, "SET", key, value, "EX", expire, "NX")
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	switch fmt.Sprintf("%v", replay) {
	case "OK":
		ok = true
	case "<nil>":
		ok = false
	}
	return
}
func (r *Redis) Set(key string, data interface{}, expire ...int) (err error) {
	b, e := Encode(data)
	if e != nil {
		return e
	}
	var conn redis.Conn
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		if len(expire) > 0 {
			_, err = r.Do(conn, "SETEX", key, expire[0], b)
		} else {
			_, err = r.Do(conn, "SET", key, b)
		}
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return
}

func (r *Redis) Del(key string) (err error) {
	var conn redis.Conn
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		_, err = r.Do(conn, "DEL", key)
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return
}

var KeyNotHave = errors.New("key not have")

func (r *Redis) Expire(key string, expire int64) error {
	conn := r.Pool.Get()
	defer conn.Close()
	ok, err := redis.Int64(redis.Int64(r.Do(conn, "EXPIRE", key, expire)))
	if err != nil || ok == 0 {
		return KeyNotHave
	}
	return nil
}

func (r *Redis) Exists(key string) error {
	conn := r.Pool.Get()
	defer conn.Close()
	ok, err := redis.Int64(redis.Int64(r.Do(conn, "Exists", key)))
	if err != nil || ok == 0 {
		return KeyNotHave
	}
	return nil
}

func (r *Redis) Ttl(key string) (int64, error) {
	var conn redis.Conn
	var ok interface{}
	var err error
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		ok, err = r.Do(conn, "TTL", key)
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	if ok == nil {
		return 0, nil
	}
	return redis.Int64(ok, err)
}

func (r *Redis) Incrby(key string, n int64) (num int64, err error) {
	var conn redis.Conn
	var ok interface{}
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		ok, err = r.Do(conn, "INCRBY", key, n)
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return reflect.ValueOf(ok).Int(), err
}

func (r *Redis) Hget(key string, field string, v interface{}) (err error) {
	var conn redis.Conn
	var data interface{}
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = r.Do(conn, "HGET", key, field)
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	if err != nil {
		return
	}
	if data != nil {
		return Decode(data, v)
	}
	return nil
}

func (r *Redis) GetRaw(key string) (interface{}, error) {
	var conn redis.Conn
	var data interface{}
	var err error
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = r.Do(conn, "GET", key)
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return data, err
}

func (r *Redis) Hdel(key string, fields ...interface{}) error {
	var conn redis.Conn
	var err error
	args := make([]interface{}, len(fields)+1)
	args[0] = key
	copy(args[1:], fields)
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		_, err = r.Do(conn, "HDEL", args...)
		_ = conn.Close()
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return err
}

func (r *Redis) HgetallString(key string) (map[string]string, error) {
	var conn redis.Conn
	var err error
	// var data interface{}
	var data []interface{}
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = redis.Values(r.Do(conn, "HGETALL", key))
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	if err != nil {
		return nil, err
	}
	if len(data)%2 != 0 {
		return nil, errors.New("redis: StringMap expects even number of values result")
	}
	m := make(map[string]string, len(data)/2)
	for i := 0; i < len(data); i += 2 {
		key, okKey := data[i].([]byte)
		value, okValue := data[i+1].([]byte)
		if !okKey || !okValue {
			return nil, errors.New("redigo: ScanMap key not a bulk string value")
		}
		m[string(key)] = string(value)
	}
	return m, nil
}

func (r *Redis) HgetallInt64(key string) (map[string]int64, error) {
	var conn redis.Conn
	var err error
	// var data interface{}
	var data []interface{}
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = redis.Values(r.Do(conn, "HGETALL", key))
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	if err != nil {
		return nil, err
	}
	if len(data)%2 != 0 {
		return nil, errors.New("redis: StringMap expects even number of values result")
	}
	m := make(map[string]int64, len(data)/2)
	for i := 0; i < len(data); i += 2 {
		key, okKey := data[i].([]byte)
		valueB, okValue := data[i+1].([]byte)
		if !okKey || !okValue {
			return nil, errors.New("redigo: ScanMap key not a bulk string value")
		}
		var value int64
		value, err = strconv.ParseInt(string(valueB), 10, 64)
		if err != nil {
			return nil, err
		}
		m[string(key)] = value
	}
	return m, nil
}

func (r *Redis) HmgetByKey(key string, args string) (interface{}, error) {
	var conn redis.Conn
	var err error
	var data interface{}
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = r.Do(conn, "HMGET", []interface{}{key, args}...)
		_ = conn.Close()
		if err == nil {
			return data, nil
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return nil, err
}

func (r *Redis) ToString(value interface{}, err error) (string, error) {
	return redis.String(value, err)
}

func (r *Redis) ToStringMap(value interface{}, err error) (map[string]string, error) {
	return redis.StringMap(value, err)
}

func (r *Redis) ToStrings(value interface{}, err error) ([]string, error) {
	return redis.Strings(value, err)
}

func (r *Redis) ToInt64s(value interface{}, err error) ([]int64, error) {
	return redis.Int64s(value, err)
}

func (r *Redis) ToInt64(value interface{}, err error) (int64, error) {
	return redis.Int64(value, err)
}

func (r *Redis) ToInts(value interface{}, err error) ([]int, error) {
	return redis.Ints(value, err)
}

func (r *Redis) ToValues(value interface{}, err error) ([]interface{}, error) {
	return redis.Values(value, err)
}

func (r *Redis) HgetallToMap(value interface{}, v interface{}) error {
	return Decode(value, v)
}

func (r *Redis) Hmget(key string, fields []interface{}, v ...interface{}) (err error) {
	fieldsN := len(fields)
	if len(v) != fieldsN {
		return errors.New("hmget params error")
	}
	args := make([]interface{}, fieldsN+1)
	args[0] = key
	copy(args[1:], fields)
	var conn redis.Conn
	var data []interface{}
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = redis.Values(r.Do(conn, "HMGET", args...))
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	if err != nil {
		return err
	}
	if fieldsN != len(data) {
		return errors.New("hmget error data")
	}
	for i, fv := range data {
		if fv == nil {
			continue
		}
		err = Decode(fv, v[i])
		if err != nil {
			s := fmt.Sprintf("Decode f=%s err=%s", fields[i], err.Error())
			return errors.New(s)
		}
	}
	return nil
}

func (r *Redis) Hset(key string, field string, data interface{}) (err error) {
	b, e := Encode(data)
	if e != nil {
		return e
	}
	var conn redis.Conn
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		_, err = r.Do(conn, "HSET", key, field, b)
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return
}

func (r *Redis) Hmset(key string, data map[string]interface{}) (err error) {
	var args []interface{}
	args = append(args, key)
	for k, v := range data {
		b, e := Encode(v)
		if e != nil {
			return e
		}
		args = append(args, k, b)
	}
	var conn redis.Conn
	var ok interface{}
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		ok, err = r.Do(conn, "HMSET", args...)
		conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	if err != nil {
		return
	}
	if reflect.ValueOf(ok).String() != "OK" {
		return errors.New("hmset err")
	}
	return nil
}

func (r *Redis) Hincrby(key string, field string, n int64) (num int64, err error) {
	var conn redis.Conn
	var ok interface{}
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		ok, err = r.Do(conn, "HINCRBY", key, field, n)
		conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	if err != nil {
		return
	}
	return reflect.ValueOf(ok).Int(), err
}

func (r *Redis) Hmincrby(key string, data map[string]int64) (ret map[string]int64, err error) {
	conn := r.Pool.Get()
	defer conn.Close()
	var fields []string
	for k, v := range data {
		err = r.Send(conn, "HINCRBY", key, k, v)
		if err != nil {
			return
		}
		fields = append(fields, k)
	}
	err = conn.Flush()
	if err != nil {
		return
	}
	ret = make(map[string]int64)
	for _, f := range fields {
		var i interface{}
		i, err = conn.Receive()
		if err != nil {
			return
		}
		ret[f] = reflect.ValueOf(i).Int()
	}
	if len(ret) != len(data) {
		err = errors.New("hmincrby reply err")
	}
	return
}

func (r *Redis) Hgetraw(key string, field string) (data interface{}, err error) {
	var conn redis.Conn
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = r.Do(conn, "HGET", key, field)
		conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return
}

func Encode(data interface{}) (b []byte, err error) {
	switch v := data.(type) {
	case []byte:
		b = data.([]byte)
	case string:
		b = []byte(v)
	case int:
		var dst []byte
		b = strconv.AppendInt(dst, int64(v), 10)
	case int8:
		var dst []byte
		b = strconv.AppendInt(dst, int64(v), 10)
	case int16:
		var dst []byte
		b = strconv.AppendInt(dst, int64(v), 10)
	case int32:
		var dst []byte
		b = strconv.AppendInt(dst, int64(v), 10)
	case int64:
		var dst []byte
		b = strconv.AppendInt(dst, v, 10)
	case uint:
		var dst []byte
		b = strconv.AppendUint(dst, uint64(v), 10)
	case uint8:
		var dst []byte
		b = strconv.AppendUint(dst, uint64(v), 10)
	case uint16:
		var dst []byte
		b = strconv.AppendUint(dst, uint64(v), 10)
	case uint32:
		var dst []byte
		b = strconv.AppendUint(dst, uint64(v), 10)
	case uint64:
		var dst []byte
		b = strconv.AppendUint(dst, v, 10)
	default:
		b, err = encode(data)
		return b, err
	}
	return
}

func Decode(data interface{}, iv interface{}) error {
	b := data.([]byte)
	switch v := iv.(type) {
	case *[]byte:
		*v = b
	case *string:
		*v = string(b)
	case *int:
		s := string(b)
		i, e := strconv.Atoi(s)
		if e != nil {
			return e
		}
		*v = i
	case *int8:
		s := string(b)
		i, e := strconv.Atoi(s)
		if e != nil {
			return e
		}
		*v = int8(i)
	case *int16:
		s := string(b)
		i, e := strconv.Atoi(s)
		if e != nil {
			return e
		}
		*v = int16(i)
	case *int32:
		s := string(b)
		i, e := strconv.Atoi(s)
		if e != nil {
			return e
		}
		*v = int32(i)
	case *int64:
		s := string(b)
		i, e := strconv.ParseInt(s, 10, 64)
		if e != nil {
			return e
		}
		*v = i
	case *uint:
		s := string(b)
		i, e := strconv.ParseUint(s, 10, 64)
		if e != nil {
			return e
		}
		*v = uint(i)
	case *uint8:
		s := string(b)
		i, e := strconv.ParseUint(s, 10, 64)
		if e != nil {
			return e
		}
		*v = uint8(i)
	case *uint16:
		s := string(b)
		i, e := strconv.ParseUint(s, 10, 64)
		if e != nil {
			return e
		}
		*v = uint16(i)
	case *uint32:
		s := string(b)
		i, e := strconv.ParseUint(s, 10, 64)
		if e != nil {
			return e
		}
		*v = uint32(i)
	case *uint64:
		s := string(b)
		i, e := strconv.ParseUint(s, 10, 64)
		if e != nil {
			return e
		}
		*v = i
	default:
		err := decode(b, iv)
		return err
	}
	return nil
}

func (r *Redis) Zadd(key string, id interface{}, score interface{}) error {
	conn := r.Pool.Get()
	defer conn.Close()
	_, err := r.Do(conn, "ZADD", key, score, id)
	return err
}

func (r *Redis) Zrem(key string, id interface{}) error {
	conn := r.Pool.Get()
	defer conn.Close()
	_, err := r.Do(conn, "ZREM", key, id)
	return err
}

func (r *Redis) Zscore(key string, id interface{}) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "ZSCORE", key, id)
	if err != nil {
		return 0, err
	}
	if data != nil {
		b := data.([]byte)
		return strconv.ParseInt(string(b), 10, 64)
	}
	return 0, nil
}

func (r *Redis) Zincrby(key string, id interface{}, n int) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "ZINCRBY", key, n, id)
	if err != nil {
		return 0, err
	}
	if data != nil {
		b := data.([]byte)
		return strconv.ParseInt(string(b), 10, 64)
	}
	return 0, nil
}

func (r *Redis) Zrank(key string, id interface{}) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "ZRANK", key, id)
	if err != nil {
		return 0, err
	}
	return data.(int64), err
}

func (r *Redis) Zrevrank(key string, id interface{}) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "ZREVRANK", key, id)
	if err != nil {
		return 0, err
	}
	if data == nil {
		return -1, nil
	}
	return data.(int64), err
}

func (r *Redis) Zrange(key string, start int, end int) (interface{}, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	var args []interface{}
	args = append(args, key)
	args = append(args, start)
	args = append(args, end)
	data, err := r.Do(conn, "ZRANGE", args...)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *Redis) Zrevrange(key string, start int, end int) ([][]string, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	var args []interface{}
	args = append(args, key)
	args = append(args, start)
	args = append(args, end)
	args = append(args, "WITHSCORES")
	data, err := r.Do(conn, "ZREVRANGE", args...)
	if err != nil {
		return nil, err
	}
	if data != nil {
		var val [][]string
		v := data.([]interface{})
		l := len(v)
		i := 0
		for {
			if i+2 > l {
				break
			}
			k := string(v[i].([]byte))
			score := string(v[i+1].([]byte))
			val = append(val, []string{k, score})
			i += 2
		}
		return val, nil
	}
	return nil, nil
}

func (r *Redis) ZrangeByScore(key string, params ...interface{}) ([]string, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	var args []interface{}
	args = append(args, key)
	args = append(args, params...)
	data, err := r.Do(conn, "ZRANGEBYSCORE", args...)
	if err != nil {
		return nil, err
	}
	if data != nil {
		var arr []string
		v := data.([]interface{})
		for _, s := range v {
			arr = append(arr, string(s.([]byte)))
		}
		return arr, nil
	}
	return nil, nil
}

func (r *Redis) ZrevrangeByScore(key string, params ...interface{}) ([]string, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	var args []interface{}
	args = append(args, key)
	args = append(args, params...)
	data, err := r.Do(conn, "ZREVRANGEBYSCORE", args...)
	if err != nil {
		return nil, err
	}
	if data != nil {
		var arr []string
		v := data.([]interface{})
		for _, s := range v {
			arr = append(arr, string(s.([]byte)))
		}
		return arr, nil
	}
	return nil, nil
}

func (r *Redis) Zcard(key string) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "ZCARD", key)
	if err != nil {
		return 0, err
	}
	if data == nil {
		return 0, nil
	}
	return data.(int64), err
}

func (r *Redis) Zcount(key string, s, e interface{}) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "ZCOUNT", key, s, e)
	if err != nil {
		return 0, err
	}
	if data == nil {
		return 0, nil
	}
	return data.(int64), err
}

func (r *Redis) Subscribe(channels []string, cb func([]byte)) error {
	conn := r.Pool.Get()
	defer conn.Close()
	psc := redis.PubSubConn{Conn: conn}
	for _, cl := range channels {
		_ = psc.Subscribe(cl)
	}
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			cb(v.Data)
		case redis.Subscription:
			log.Println("[info] subscribe", v.Channel, v.Kind, v.Count)
		case error:
			return v
		}
	}
}

func (r *Redis) Publish(channel string, msg []byte) error {
	conn := r.Pool.Get()
	defer conn.Close()
	_, err := r.Do(conn, "PUBLISH", msg)
	return err
}

func (r *Redis) Lpush(key string, v interface{}) error {
	conn := r.Pool.Get()
	defer conn.Close()
	b, err := Encode(v)
	if err != nil {
		return err
	}
	_, err = r.Do(conn, "LPUSH", key, b)
	return err
}

func (r *Redis) Rpush(key string, v interface{}) error {
	conn := r.Pool.Get()
	defer conn.Close()
	b, err := Encode(v)
	if err != nil {
		return err
	}
	_, err = r.Do(conn, "RPUSH", key, b)
	return err
}

func (r *Redis) Lpop(key string, v interface{}) error {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "LPOP", key)
	if data != nil && v != nil {
		return Decode(data, v)
	}
	return err
}
func (r *Redis) Rpop(key string, v interface{}) error {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "RPOP", key)
	if data != nil && v != nil {
		return Decode(data, v)
	}
	return err
}

func (r *Redis) Lrange(key string, start interface{}, offset interface{}) ([]interface{}, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	var args []interface{}
	args = append(args, key)
	args = append(args, start)
	args = append(args, offset)
	data, err := r.Do(conn, "LRANGE", args...)
	if data != nil {
		return data.([]interface{}), nil
	}
	return nil, err
}

func (r *Redis) Llen(key string) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "LLEN", key)
	if err != nil {
		return 0, err
	}
	if data == nil {
		return 0, nil
	}
	return data.(int64), err
}

func (r *Redis) Lock(key string, value interface{}, expire int64) (bool, error) {
	conn := r.Pool.Get()
	defer conn.Close()

	ok, err := redis.String(r.Do(conn, "SET", key, value, "EX", expire, "NX"))
	if err != nil || ok != "OK" {

		return false, err
	}

	return true, nil
}

func (r *Redis) CheckLock(key string) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "Get", key)
	if err != nil {
		return 0, err
	}
	if data == nil {
		return 0, err
	}
	return redis.Int64(data, err)
}

func (r *Redis) Unlock(key string) (err error) {
	conn := r.Pool.Get()
	defer conn.Close()
	_, err = r.Do(conn, "del", key)
	return
}

type Command struct {
	Cmd    string
	Args   []interface{}
	Err    error
	Result interface{}
	Tag    int
}

func (c *Command) ResultInt() (int64, error) {
	if c.Result == nil {
		return 0, errors.New("command result nil")
	}
	return reflect.ValueOf(c.Result).Int(), nil
}

func (c *Command) ResultBool() (bool, error) {
	return redis.Bool(c.Result, c.Err)
}

type PipeLine struct {
	Commands []*Command
	RunErr   error
	WithTag  bool
}

func (pipe *PipeLine) Append(cmd string, args ...interface{}) {
	pipe.Commands = append(pipe.Commands, &Command{Cmd: cmd, Args: args})
}

func (pipe *PipeLine) AppendWithTag(cmd string, tag int, args ...interface{}) {
	pipe.WithTag = true
	pipe.Commands = append(pipe.Commands, &Command{Cmd: cmd, Tag: tag, Args: args})
}

func (pipe *PipeLine) GetCommendByTag(tag int) (*Command, bool) {
	for _, c := range pipe.Commands {
		if c.Tag == tag {
			return c, true
		}
	}
	return nil, false
}

func (r *Redis) NewPipeLine() *PipeLine {
	return new(PipeLine)
}

func (r *Redis) RunPipeLine(pipe *PipeLine) bool {
	conn := r.Pool.Get()
	defer conn.Close()
	var err error
	for _, value := range pipe.Commands {
		err = r.Send(conn, value.Cmd, value.Args...)
		if err != nil {
			pipe.RunErr = err
			return false
		}
	}
	err = conn.Flush()
	if err != nil {
		pipe.RunErr = err
		return false
	}
	ok := true
	for _, value := range pipe.Commands {
		value.Result, value.Err = conn.Receive()
		if value.Err != nil {
			ok = false
		}
	}
	return ok
}

func (r *Redis) Sadd(key string, members ...interface{}) (ok bool, err error) {
	var conn redis.Conn
	args := make([]interface{}, len(members)+1)
	args[0] = key
	copy(args[1:], members)
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		ok, err = redis.Bool(r.Do(conn, "SADD", args...))
		conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return ok, err
}

func (r *Redis) Srem(key string, members ...interface{}) error {
	var conn redis.Conn
	var err error
	args := make([]interface{}, len(members)+1)
	args[0] = key
	copy(args[1:], members)
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		_, err = r.Do(conn, "SREM", args...)
		conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return err
}

func (r *Redis) Sismember(key string, member interface{}) (data bool, err error) {
	var conn redis.Conn
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = redis.Bool(r.Do(conn, "SISMEMBER", key, member))
		conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return
}

func (r *Redis) Srandmembers(key string) (data interface{}, err error) {
	var conn redis.Conn
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = r.Do(conn, "SRANDMEMBER", key)
		conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return
}

func (r *Redis) Smembers(key string) (data interface{}, err error) {
	var conn redis.Conn
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = r.Do(conn, "SMEMBERS", key)
		conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return
}
func (r *Redis) Sdiff(key1, key2 string) (data interface{}, err error) {
	var conn redis.Conn
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		data, err = r.Do(conn, "SDIFF", key1, key2)
		conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return
}

func (r *Redis) Scard(key string) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	data, err := r.Do(conn, "SCARD", key)
	if err != nil {
		return 0, err
	}
	if data == nil {
		return 0, nil
	}
	return data.(int64), err
}

func (r *Redis) Keys(keyPattern string) (res []string, err error) {
	res = []string{}
	var conn redis.Conn
	conn = r.Pool.Get()
	var resValue interface{}
	resValue, err = r.Do(conn, "KEYS", keyPattern)
	_ = conn.Close()
	s, ok := resValue.([]interface{})
	if !ok {
		return
	}
	res = make([]string, 0, len(s))
	var keyBt []byte
	for _, i2 := range s {
		keyBt, ok = i2.([]byte)
		res = append(res, string(keyBt))
	}
	return

}
func (r *Redis) DelByKeyPattern(keyPattern string) (err error) {
	var keys []string
	keys, err = r.Keys(keyPattern)
	if err != nil {
		return
	}
	if len(keys) > 0 {
		err = r.DelKeys(keys...)
	}

	return
}
func (r *Redis) parseSliceStringToSliceInterface(arg []string) (res []interface{}) {
	res = make([]interface{}, 0, len(arg))
	for _, s := range arg {
		res = append(res, s)
	}
	return
}

// 批量删除KEY
func (r *Redis) DelKeys(key ...string) (err error) {
	keys := r.parseSliceStringToSliceInterface(key)
	if len(keys) == 0 {
		return
	}
	var conn redis.Conn
	for i := 0; i < 2; i++ {
		conn = r.Pool.Get()
		_, err = r.Do(conn, "DEL", keys...)
		_ = conn.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return
}

func (r *Redis) Ltrim(key string, start, to int) (err error) {
	conn := r.Pool.Get()
	defer conn.Close()
	_, err = r.Do(conn, "LTRIM", key, start, to)
	return err
}

func (r *Redis) Do(conn redis.Conn, commandName string, args ...interface{}) (reply interface{}, err error) {
	reply, err = conn.Do(commandName, args...)
	return reply, err
}

func (r *Redis) Send(conn redis.Conn, commandName string, args ...interface{}) error {
	err := conn.Send(commandName, args...)
	return err
}

var (
	msgpackHandle codec.MsgpackHandle
)

func encode(v interface{}) ([]byte, error) {
	byteBuf := new(bytes.Buffer)
	enc := codec.NewEncoder(byteBuf, &msgpackHandle)
	err := enc.Encode(v)
	return byteBuf.Bytes(), err
}

func decode(data []byte, v interface{}) error {
	dec := codec.NewDecoder(bytes.NewReader(data), &msgpackHandle)
	return dec.Decode(v)
}
