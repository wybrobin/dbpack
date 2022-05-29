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

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/storage/etcd"
	"github.com/cectc/dbpack/pkg/executor"
	"github.com/cectc/dbpack/pkg/filter"
	_ "github.com/cectc/dbpack/pkg/filter/dt"
	_ "github.com/cectc/dbpack/pkg/filter/metrics"
	dbpackHttp "github.com/cectc/dbpack/pkg/http"
	"github.com/cectc/dbpack/pkg/listener"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/pkg/server"
	"github.com/cectc/dbpack/third_party/pools"
	_ "github.com/cectc/dbpack/third_party/types/parser_driver"
)

func main() {
	rootCommand.Execute()
}

var (
	Version               = "0.1.0"
	defaultHTTPListenPort = 18888

	configPath string

	rootCommand = &cobra.Command{
		Use:     "dbpack",
		Short:   "dbpack is a db proxy server",
		Version: Version,
	}

	startCommand = &cobra.Command{
		Use:   "start",
		Short: "start dbpack",

		Run: func(cmd *cobra.Command, args []string) {
			//h := initHolmes()
			//h.Start()
			conf := config.Load(configPath)

			//将配置文件里的filters下的列表里的配置，根据kind拿到不通的FilterFactory，再由FilterFactory构建出不同的Filter，以name为key注册map到pkg/filter/filters变量里
			for _, filterConf := range conf.Filters {
				factory := filter.GetFilterFactory(filterConf.Kind)
				if factory == nil {
					log.Fatalf("there is no filter factory for filter: %s", filterConf.Kind)
				}
				f, err := factory.NewFilter(filterConf.Config)
				if err != nil {
					log.Fatal(errors.WithMessagef(err, "failed to create filter: %s", filterConf.Name))
				}
				filter.RegisterFilter(filterConf.Name, f)
			}

			//根据data_source_cluster配置，对应filters配置，初始化资源池，构建 pkg/resource/data_source.go 里的 dbManager
			resource.InitDBManager(conf.DataSources, func(dbName, dsn string) pools.Factory {
				collector, err := driver.NewConnector(dbName, dsn)
				if err != nil {
					log.Fatal(err)
				}
				return collector.NewBackendConnection
			})

			executors := make(map[string]proto.Executor)
			for _, executorConf := range conf.Executors {
				if executorConf.Mode == config.SDB {	//只连接一个数据库
					executor, err := executor.NewSingleDBExecutor(executorConf)
					if err != nil {
						log.Fatal(err)
					}
					executors[executorConf.Name] = executor
				}
				if executorConf.Mode == config.RWS {	//读写分离的数据库
					executor, err := executor.NewReadWriteSplittingExecutor(executorConf)
					if err != nil {
						log.Fatal(err)
					}
					executors[executorConf.Name] = executor
				}
				if executorConf.Mode == config.SHD {	//hash分片的数据库
					executor, err := executor.NewShardingExecutor(executorConf)
					if err != nil {
						log.Fatal(err)
					}
					executors[executorConf.Name] = executor
				}
			}

			//初始化分布式事务，主要是连接etcd。当多进程的时候，在共用一个appid的进程中，使用etcd选取出一个主进程，运行一些全局任务
			if conf.DistributedTransaction != nil {
				driver := etcd.NewEtcdStore(conf.DistributedTransaction.EtcdConfig)
				dt.InitDistributedTransactionManager(conf.DistributedTransaction, driver)
			}

			dbpack := server.NewServer()

			//都是使用net.Listen监听，mysql的listener多个executor，因为mysql要透传到数据库，http的listener多个filters，因为http只让合法的路径通过
			for _, listenerConf := range conf.Listeners {
				switch listenerConf.ProtocolType {
				case config.Mysql:	//如果是mysql的listener，则根据executor匹配executors里面的name
					listener, err := listener.NewMysqlListener(listenerConf)
					if err != nil {
						log.Fatalf("create mysql listener failed %v", err)
					}
					dbListener := listener.(proto.DBListener)
					executor := executors[listenerConf.Executor]
					if executor == nil {
						log.Fatalf("executor: %s is not exists for mysql listener", listenerConf.Executor)
					}
					dbListener.SetExecutor(executor)
					dbpack.AddListener(dbListener)
				case config.Http:
					listener, err := listener.NewHttpListener(listenerConf)
					if err != nil {
						log.Fatalf("create http listener failed %v", err)
					}
					dbpack.AddListener(listener)
				default:
					log.Fatalf("unsupported %v listener protocol type", listenerConf.ProtocolType)
				}
			}

			//当进程收到 SIGINT 或 SIGTERM 信号时，根据 termination_drain_duration 配置（sample里没有），晚一点退出。如果连续2次收到，就直接退出
			ctx, cancel := context.WithCancel(context.Background())
			c := make(chan os.Signal, 2)
			signal.Notify(c, os.Interrupt, syscall.SIGTERM)
			go func() {
				<-c
				go func() {
					// cancel server after sleeping `TerminationDrainDuration`
					// cancel asynchronously to avoid blocking the second term signal
					time.Sleep(conf.TerminationDrainDuration)
					cancel()
				}()
				<-c
				os.Exit(1) // second signal. Exit directly.
			}()

			// init metrics for prometheus server scrape.
			// default listen at 18888
			//给 prometheus 用来监控的服务，实现/live、/ready和/metrics接口
			var lis net.Listener
			var lisErr error
			if conf.HTTPListenPort != nil {
				lis, lisErr = net.Listen("tcp4", fmt.Sprintf(":%d", *conf.HTTPListenPort))
			} else {
				lis, lisErr = net.Listen("tcp4", fmt.Sprintf(":%d", defaultHTTPListenPort))
			}

			if lisErr != nil {
				log.Fatalf("unable init metrics server: %+v", lisErr)
			}
			//启动 prometheus 用来监控的http服务
			go initServer(ctx, lis)

			//启动listener
			dbpack.Start(ctx)
		},
	}
)

// init Init startCmd
func init() {
	startCommand.PersistentFlags().StringVarP(&configPath, constant.ConfigPathKey, "c", os.Getenv(constant.EnvDBPackConfig), "Load configuration from `FILE`")
	rootCommand.AddCommand(startCommand)
}

func initServer(ctx context.Context, lis net.Listener) {
	go func() {
		<-ctx.Done()
		lis.Close()
	}()
	handler, err := dbpackHttp.RegisterRoutes()
	if err != nil {
		log.Fatalf("failed to init handler: %+v", err)
		return
	}
	httpS := &http.Server{
		Handler: handler,
	}
	err = httpS.Serve(lis)
	if err != nil {
		log.Fatalf("unable create status server: %+v", err)
		return
	}
	log.Infof("start api server :  %s", lis.Addr())
}

//func initHolmes() *holmes.Holmes {
//	logUtils.DefaultLogger.SetLogLevel(logUtils.ERROR)
//	h, _ := holmes.New(
//		holmes.WithCollectInterval("5s"),
//		holmes.WithDumpPath("/tmp"),
//		holmes.WithCPUDump(20, 25, 80, time.Minute),
//		holmes.WithCPUMax(90),
//	)
//	h.EnableCPUDump()
//	return h
//}
