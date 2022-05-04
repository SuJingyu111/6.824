package shardkv

import (
	"time"
)

func (kv *ShardKV) pullLatestConfig() {
	for !kv.killed() {
		if !kv.rf.IsLeader() {
			time.Sleep(PULL_CONFIG_TMO * time.Millisecond)
		} else {
			latestConfig := kv.ctrlerInterface.Query(-1)
			if latestConfig.Num > kv.config.Num {
				op := Op{
					Type:      INSTALL_CONFIG,
					NewConfig: latestConfig,
				}
				_, _, isLeader := kv.rf.Start(op)
				if !isLeader {
					time.Sleep(PULL_CONFIG_TMO * time.Millisecond)
					continue
				}
			} else if latestConfig.Num == kv.config.Num {
				DPrintf("SERVER_PULL_CONFIG_LOOP: Config already up to date")
			} else {
				DPrintf("SERVER_PULL_CONFIG_LOOP: ERROR!!!!!!!!!!!!!!: LATEST CONFIG LOWER NUM")
			}
			time.Sleep(PULL_CONFIG_TMO * time.Millisecond)
		}
	}
}
