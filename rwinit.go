package rwlock

import (
	"github.com/lzw5399/rwlock/client"
)

// Init
// 初始化redis客户端
func Init(optObj interface{}) {
	// redis属于基础资源 如果redis的客户端初始化失败，直接panic，没得商量
	if err := client.DoInit(optObj); err != nil {
		panic("redis client init ")
	}
}
