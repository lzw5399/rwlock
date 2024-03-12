package client

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/lzw5399/rwlock/lua"
	"github.com/lzw5399/rwlock/tool"
)

var Redis redis.UniversalClient
var opts interface{}

// error 定义
const NoScriptError = "NOSCRIPT No matching script. Please use EVAL."
const EofError = "EOF"

// 锁的相关指令
const LockCmd = "LOCK"
const UnlockCmd = "UNLOCK"
const RLockCmd = "RLOCK"
const RUnlockCmd = "RUNLOCK"

var shaHashID string

func DoInit(optObj interface{}) error {
	switch opt := optObj.(type) {
	case *redis.Options:
		Redis = redis.NewClient(opt)
	case *redis.FailoverOptions:
		Redis = redis.NewFailoverClient(opt)
	case *redis.ClusterOptions:
		Redis = redis.NewClusterClient(opt)
	default:
		return errors.New("unsupported options")
	}
	if _, err := Redis.Ping(context.Background()).Result(); err != nil {
		return err
	}
	opts = optObj

	if err := LoadLua(); err != nil {
		return err
	}
	return nil
}

// 加载 Lua脚本
func LoadLua() error {
	hashID, err := Redis.ScriptLoad(context.Background(), lua.ScriptContent).Result()
	if err != nil {
		return err
	}
	// 保存hashID
	SetShaHasID(hashID)
	return nil

}
func GetShaHashID() string {
	return shaHashID
}
func SetShaHasID(str string) {
	shaHashID = str
}

// responseLock
// 收到redis的指令回馈
type responseLock struct {
	OpRet  bool   `json:"opRet"`
	ErrMsg string `json:"errMsg"`
	Debug  string `json:"debug"`
}

func (r responseLock) IsError() bool {
	if len(r.ErrMsg) > 0 {
		return true
	}
	return false
}
func (r responseLock) Success() bool {
	return r.OpRet
}
func (r responseLock) Error() string {
	return r.ErrMsg
}

// Lock
// 写锁
func Lock(key string, uniqID string, expireTime int64) {
	if len(key) < 0 {
		panic("lock key is nil")
	}
	if expireTime <= 0 {
		expireTime = 5
	}
	for {
		res, err := sendLock(GetShaHashID(), key, uniqID, LockCmd, expireTime)
		if err != nil {
			handleError(err)
			time.Sleep(getRandomSleepTime())
			continue
		}
		if res != nil && res.IsError() {
			panic(res.Error())
		}
		if res != nil && res.Success() {
			return
		}

		time.Sleep(getRandomSleepTime())
	}
}

// Unlock
// 写锁的释放
func Unlock(key, uniqID string) {
	i := 10
	for {
		res, err := sendLock(GetShaHashID(), key, uniqID, UnlockCmd, 0)
		if res != nil && res.Success() {
			return
		}
		if res != nil && res.IsError() {
			panic(res.Error())
		}
		if err != nil {
			handleError(err)
		}
		if i--; i <= 0 {
			return
		}
		time.Sleep(getRandomSleepTime())
	}
}

// RLock
// 读锁
func RLock(key string) {
	for {
		res, err := sendLock(GetShaHashID(), key, "", RLockCmd, 0)
		if res != nil && res.Success() {
			return
		}
		if err != nil {
			handleError(err)
		}

		time.Sleep(getRandomSleepTime())
	}
}

// RUnlock
// 释放读锁
func RUnlock(key string) {
	if len(key) <= 0 {
		panic("runlock nil key")
	}
	i := 10
	for {
		res, err := sendLock(GetShaHashID(), key, "", RUnlockCmd, 0)
		if res != nil && res.Success() {
			return
		}
		if err != nil {
			handleError(err)
		}

		if i--; i <= 0 {
			return
		}
		time.Sleep(getRandomSleepTime())
	}
}

// getRandomSleepTime
// 随机 睡眠时间
// 10 - 20 ms
func getRandomSleepTime() time.Duration {
	return time.Duration(tool.Rand(10, 20)) * time.Millisecond
}

// sendLock
// 发送封装并发送锁指令
func sendLock(shaHashID, key string, uniqID, lockCmd string, expireTime int64) (*responseLock, error) {
	var ret interface{}
	var err error
	switch lockCmd {
	case LockCmd:
		ret, err = Redis.EvalSha(context.Background(), shaHashID, []string{key, lockCmd}, []string{uniqID, strconv.Itoa(int(expireTime))}).Result()
	case UnlockCmd:
		ret, err = Redis.EvalSha(context.Background(), shaHashID, []string{key, lockCmd}, []string{uniqID}).Result()
	case RLockCmd, RUnlockCmd:
		ret, err = Redis.EvalSha(context.Background(), shaHashID, []string{key, lockCmd}, []string{uniqID}).Result()
	}

	if err != nil {
		return nil, err
	}
	var retJson = ret.(string)
	var res responseLock
	if err := json.Unmarshal([]byte(retJson), &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// handleError
// 统一处理错误信息
func handleError(err error) bool {
	if err == nil {
		return false
	}
	switch err.Error() {
	case EofError:
		// 收到了Eof，redis服务重启
		if err := handleEofError(); err != nil {
			return false
		}
		return true
	case NoScriptError:
		// redis没有找到对应的Lua脚本
		if err := handleNoScriptError(); err != nil {
			return false
		}
		return true
	default:
		return false
	}

}

// redis重启
// 重试初始化一次
func handleEofError() error {
	return DoInit(opts)
}

// Lua script 不存在
// 重新Load一下Lua
func handleNoScriptError() error {
	return LoadLua()
}
