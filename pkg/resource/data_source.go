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

package resource

import (
	"fmt"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/sql"
	"github.com/cectc/dbpack/third_party/pools"
)

var dbManager proto.DBManager

type DBManager struct {
	dataSources   []*config.DataSource
	resourcePools map[string]proto.DB
}

func InitDBManager(dataSources []*config.DataSource, factory func(dbName, dsn string) pools.Factory) {
	resourcePools := make(map[string]proto.DB, 0)

	initResourcePool := func(dataSourceConfig *config.DataSource) *pools.ResourcePool {
		resourcePool := pools.NewResourcePool(factory(dataSourceConfig.Name, dataSourceConfig.DSN), dataSourceConfig.Capacity,
			dataSourceConfig.MaxCapacity, dataSourceConfig.IdleTimeout, 0, nil)
		return resourcePool
	}

	for i := 0; i < len(dataSources); i++ {
		var (
			connectionPreFilters  []proto.DBConnectionPreFilter
			connectionPostFilters []proto.DBConnectionPostFilter
		)
		dataSource := dataSources[i]
		resourcePool := initResourcePool(dataSource)	//初始化了一个通过chan的生成的资源池，Put和Get对资源池进行操作
		db := sql.NewDB(dataSource.Name, dataSource.PingInterval, dataSource.PingTimesForChangeStatus, resourcePool)
		for j := 0; j < len(dataSource.Filters); j++ {//data_source_cluster.filters
			filterName := dataSource.Filters[j]
			f := filter.GetFilter(filterName)	//匹配 filters.name
			if f != nil {
				preFilter, ok := f.(proto.DBConnectionPreFilter)	//就是实现了 PreHandle 接口的 Filter，mysql、connectionMetric 都实现了，
																	// http实现的参数不同，属于 HttpPreFilter，可以在pkg/proto/interface.go通过跳转查看
				if ok {
					connectionPreFilters = append(connectionPreFilters, preFilter)
				}
				postFilter, ok := f.(proto.DBConnectionPostFilter)	//就是实现了 PostHandle 接口的 Filter，mysql、connectionMetric 都实现了，
																	// http实现的参数不同，属于 HttpPostFilter，可以在pkg/proto/interface.go通过跳转查看
				if ok {
					connectionPostFilters = append(connectionPostFilters, postFilter)
				}
			}
		}

		db.SetConnectionPreFilters(connectionPreFilters)
		db.SetConnectionPostFilters(connectionPostFilters)
		resourcePools[dataSource.Name] = db
	}
	dbManager = &DBManager{
		dataSources:   dataSources,
		resourcePools: resourcePools,
	}
}

func GetDBManager() proto.DBManager {
	return dbManager
}

func SetDBManager(manager proto.DBManager) {
	dbManager = manager
}

func (manager *DBManager) GetDB(name string) proto.DB {
	return manager.resourcePools[name]
}

func (manager *DBManager) GetResourcePoolStatus() error {
	for _, dataSource := range manager.resourcePools {
		db := dataSource.(*sql.DB)
		if err := db.TestConn(); err != nil {
			return fmt.Errorf("datasource %s is not ready, err: %+v", db.Name(), err)
		}
	}
	return nil
}
