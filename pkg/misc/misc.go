/*
 * Copyright 2022 CECTC, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package misc

import (
	"fmt"
	"strings"
)
//将order^^^`order`.`so_master`^^^A,B 拆分成order^^^`order`.`so_master`^^^A和order^^^`order`.`so_master`^^^B
func CollectRowKeys(lockKey, resourceID string) []string {
	var locks = make([]string, 0)
	tableGroupedLockKeys := strings.Split(lockKey, ";")	//当锁多个值的时候，用;隔开，这里拆分成切片
	for _, tableGroupedLockKey := range tableGroupedLockKeys {
		if tableGroupedLockKey != "" {
			idx := strings.Index(tableGroupedLockKey, ":")
			if idx < 0 {
				return nil
			}

			tableName := tableGroupedLockKey[0:idx]	//通过:拆分tablename，例如：`order`.`so_master`
			mergedPKs := tableGroupedLockKey[idx+1:]	//通过:拆分出主键值，例如：3742195289

			if mergedPKs == "" {
				return nil
			}

			pks := strings.Split(mergedPKs, ",")
			if len(pks) == 0 {
				return nil
			}

			for _, pk := range pks {
				if pk != "" {
					locks = append(locks, GetRowKey(resourceID, tableName, pk))	//拼接字符串，例如：order^^^`order`.`so_master`^^^3742195289
				}
			}
		}
	}
	return locks
}

func GetRowKey(resourceID string, tableName string, pk string) string {
	return fmt.Sprintf("%s^^^%s^^^%s", resourceID, tableName, pk)
}
