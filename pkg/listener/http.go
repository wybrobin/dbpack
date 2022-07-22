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

package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/textproto"

	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel/propagation"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/tracing"
)

type HttpConfig struct {
	BackendHost string `yaml:"backend_host" json:"backend_host"`
}

type HttpListener struct {
	conf HttpConfig

	// This is the main listener socket.
	listener net.Listener

	preFilters  []proto.HttpPreFilter
	postFilters []proto.HttpPostFilter
}

func NewHttpListener(conf *config.Listener) (proto.Listener, error) {
	var (
		err     error
		content []byte
		cfg     HttpConfig
	)

	if content, err = json.Marshal(conf.Config); err != nil {
		return nil, errors.Wrap(err, "marshal http listener config failed.")
	}
	if err = json.Unmarshal(content, &cfg); err != nil {
		log.Errorf("unmarshal http listener config failed, %s", err)
		return nil, err
	}

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.SocketAddress.Address, conf.SocketAddress.Port))
	if err != nil {
		log.Errorf("listen %s:%d error, %s", conf.SocketAddress.Address, conf.SocketAddress.Port, err)
		return nil, err
	}

	listener := &HttpListener{
		conf:        cfg,
		listener:    l,
		preFilters:  make([]proto.HttpPreFilter, 0),
		postFilters: make([]proto.HttpPostFilter, 0),
	}

	for i := 0; i < len(conf.Filters); i++ {
		filterName := conf.Filters[i]
		f := filter.GetFilter(filterName)
		if f != nil {
			preFilter, ok := f.(proto.HttpPreFilter)
			if ok {
				listener.preFilters = append(listener.preFilters, preFilter)
			}
			postFilter, ok := f.(proto.HttpPostFilter)
			if ok {
				listener.postFilters = append(listener.postFilters, postFilter)
			}
		}
	}
	return listener, nil
}

func (l *HttpListener) Listen() {
	log.Infof("start http listener %s", l.listener.Addr())
	if err := fasthttp.Serve(l.listener, func(fastHttpCtx *fasthttp.RequestCtx) {
		//这个也就是作为http服务的代理，转发请求前做的事情
		fastHttpCtx.SetUserValue(dt.VarHost, l.conf.BackendHost)
		ctx := extractTraceContext(context.Background(), &fastHttpCtx.Request)
		spanCtx, span := tracing.GetTraceSpan(ctx, tracing.HTTPProxyService)
		defer span.End()

		if err := l.doPreFilter(spanCtx, fastHttpCtx); err != nil {	//调用 HttpDistributedTransaction 的 PreHandle 方法
			tracing.RecordErrorSpan(span, err)
			log.Error(err)
			return
		}
		request := &fasthttp.Request{}	//创建新的request
		fastHttpCtx.Request.CopyTo(request)	//将旧的request拷贝到新的里面

		// inject trace info.
		carrier := propagation.MapCarrier{}
		injectTraceContext(spanCtx, carrier)
		for k, v := range carrier {
			log.Debugf("trace carrier key: %s, value: %s", k, v)
			request.Header.Set(k, v)
		}

		request.SetHost(l.conf.BackendHost)		//设置新的请求地址
		//这里才真正调用被代理的http服务，sample里的 aggregation_svc
		if err := fasthttp.Do(request, &fastHttpCtx.Response); err != nil {
			log.Error(err)
		}
		//调用 HttpDistributedTransaction 的 PostHandle 方法，也就是调用完被代理的http服务后，要做的一些事情
		if err := l.doPostFilter(spanCtx, fastHttpCtx); err != nil {
			tracing.RecordErrorSpan(span, err)
			log.Error(err)
			fastHttpCtx.Response.Reset()
			fastHttpCtx.SetStatusCode(http.StatusInternalServerError)
			fastHttpCtx.SetBodyString(fmt.Sprintf(`{"success":false,"error":"%s"}`, err.Error()))
		}
	}); err != nil {
		log.Error(err)
	}
}

func (l *HttpListener) Close() {
	if err := l.listener.Close(); err != nil {
		log.Error(err)
	}
}

func (l *HttpListener) doPreFilter(ctx context.Context, fastHttpCtx *fasthttp.RequestCtx) error {
	for i := 0; i < len(l.preFilters); i++ {
		f := l.preFilters[i]
		err := f.PreHandle(ctx, fastHttpCtx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *HttpListener) doPostFilter(ctx context.Context, fastHttpCtx *fasthttp.RequestCtx) error {
	for i := 0; i < len(l.postFilters); i++ {
		f := l.postFilters[i]
		err := f.PostHandle(ctx, fastHttpCtx)
		if err != nil {
			return err
		}
	}
	return nil
}

// SpanContextFromRequest extracts a span context from incoming requests.
func extractTraceContext(ctx context.Context, req *fasthttp.Request) context.Context {
	h, ok := getRequestHeader(req, tracing.TraceParentHeader)
	tc := propagation.TraceContext{}
	carrier := propagation.MapCarrier{}
	if ok {
		carrier.Set(tracing.TraceParentHeader, h)
	}
	return tc.Extract(ctx, carrier)
}

func injectTraceContext(ctx context.Context, carrier propagation.MapCarrier) {
	tc := propagation.TraceContext{}
	tc.Inject(ctx, carrier)
}

func getRequestHeader(req *fasthttp.Request, name string) (string, bool) {
	s := string(req.Header.Peek(textproto.CanonicalMIMEHeaderKey(name)))
	if s == "" {
		return "", false
	}

	return s, true
}
