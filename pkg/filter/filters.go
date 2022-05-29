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

package filter

import "github.com/cectc/dbpack/pkg/proto"

var (
	filterFactories = make(map[string]proto.FilterFactory)	//存储了程序支持的FilterFactory，在init中以kind为key，调用RegistryFilterFactory注册进来
	filters         = make(map[string]proto.Filter)	//存储了配置文件中所有filters，key是name
)

func RegistryFilterFactory(kind string, factory proto.FilterFactory) {
	filterFactories[kind] = factory
}

func GetFilterFactory(kind string) proto.FilterFactory {
	return filterFactories[kind]
}

func RegisterFilter(name string, filter proto.Filter) {
	filters[name] = filter
}

func GetFilter(name string) proto.Filter {
	return filters[name]
}
