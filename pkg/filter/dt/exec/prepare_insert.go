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

package exec

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type prepareInsertExecutor struct {
	conn   *driver.BackendConnection
	stmt   *ast.InsertStmt
	args   map[string]interface{}
	result proto.Result
}

func NewPrepareInsertExecutor(
	conn *driver.BackendConnection,
	stmt *ast.InsertStmt,
	args map[string]interface{},
	result proto.Result) Executor {
	return &prepareInsertExecutor{
		conn:   conn,
		stmt:   stmt,
		args:   args,
		result: result,
	}
}

func (executor *prepareInsertExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *prepareInsertExecutor) AfterImage(ctx context.Context) (*schema.TableRecords, error) {
	var afterImage *schema.TableRecords	//定义返回值
	var err error
	pkValues, err := executor.getPKValuesByColumn(ctx)	//拿到插入数据主键的值
	if err != nil {
		return nil, err
	}
	if executor.getPKIndex(ctx) >= 0 {
		afterImage, err = executor.buildTableRecords(ctx, pkValues)	//afterImage 是 TableRecords 类型
	} else {
		pk, _ := executor.result.LastInsertId()
		afterImage, err = executor.buildTableRecords(ctx, []interface{}{pk})
	}
	if err != nil {
		return nil, err
	}
	return afterImage, nil
}
//返回表的列信息和索引信息
func (executor *prepareInsertExecutor) GetTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()	//获取操作的数据库的名字
	db := resource.GetDBManager().GetDB(dbName)	//根据数据库名字获取连接库的db对象，对应配置 data_source_cluster
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())	//返回表的列信息和索引信息
}
//这个不是很懂？？？反正是能拿到表名，格式：`order`.`so_master`
func (executor *prepareInsertExecutor) GetTableName() string {
	var sb strings.Builder
	if err := executor.stmt.Table.TableRefs.Left.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}
//根据主键查询表数据，组成 TableRecords 结构
func (executor *prepareInsertExecutor) buildTableRecords(ctx context.Context, pkValues []interface{}) (*schema.TableRecords, error) {
	tableMeta, err := executor.GetTableMeta(ctx)
	if err != nil {
		return nil, err
	}
	//SELECT appid,buyer_user_sysno,create_user,delivery_date,gmt_create,gmt_modified,memo,modify_user,order_date,payment_date,payment_type,receive_address,receive_contact,receive_contact_phone,receive_date,receive_division_sysno,receive_zip,seller_company_code,so_amt,so_id,status,stock_sysno,sysno FROM `order`.`so_master`  WHERE `sysno` IN (?)
	afterImageSql := executor.buildAfterImageSql(tableMeta, pkValues)
	result, _, err := executor.conn.PrepareQueryArgs(afterImageSql, pkValues)
	if err != nil {
		return nil, err
	}
	return schema.BuildBinaryRecords(tableMeta, result), nil
}

func (executor *prepareInsertExecutor) buildAfterImageSql(tableMeta schema.TableMeta, pkValues []interface{}) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	columnCount := len(tableMeta.Columns)	//总共多少列，乱序的
	for i, column := range tableMeta.Columns {
		b.WriteString(misc.CheckAndReplace(column))	//检查是不是mysql的关键词，如果是的话，则加上``
		if i < columnCount-1 {
			b.WriteByte(',')
		} else {	//是最后一行，写空格
			b.WriteByte(' ')
		}
	}
	b.WriteString(fmt.Sprintf("FROM %s ", executor.GetTableName()))	//from 表名
	b.WriteString(fmt.Sprintf("WHERE `%s` IN ", tableMeta.GetPKName()))	//where 主键名 in
	b.WriteString(misc.MysqlAppendInParam(len(pkValues)))	////组成(?,?,?,...,?)
	return b.String()
}
//根据一系列操作，最后拿到插入数据主键的值
func (executor *prepareInsertExecutor) getPKValuesByColumn(ctx context.Context) ([]interface{}, error) {
	pkValues := make([]interface{}, 0)
	columnLen := executor.getColumnLen(ctx)	//获取插入的列数
	pkIndex := executor.getPKIndex(ctx)	//返回主键的下标
	for key, value := range executor.args {
		i, err := strconv.Atoi(key[1:])
		if err != nil {
			return nil, err
		}
		if i%columnLen == pkIndex+1 {
			pkValues = append(pkValues, value)
		}
	}
	return pkValues, nil
}
//返回主键的下标
func (executor *prepareInsertExecutor) getPKIndex(ctx context.Context) int {
	insertColumns := executor.GetInsertColumns()	//插入的列数
	tableMeta, _ := executor.GetTableMeta(ctx)

	for i, columnName := range insertColumns {
		if strings.EqualFold(tableMeta.GetPKName(), columnName) {
			return i
		}
	}

	allColumns := tableMeta.Columns
	for i, column := range allColumns {
		if strings.EqualFold(tableMeta.GetPKName(), column) {
			return i
		}
	}
	return -1
}

//获取插入的列数
func (executor *prepareInsertExecutor) getColumnLen(ctx context.Context) int {
	insertColumns := executor.GetInsertColumns()
	if insertColumns != nil {
		return len(insertColumns)
	}
	tableMeta, _ := executor.GetTableMeta(ctx)

	return len(tableMeta.Columns)
}

//对于语句:INSERT /*+ XID('gs/aggregationSvc/9169597303674880002') */ INTO order.so_master (sysno, so_id, buyer_user_sysno, seller_company_code,
//                receive_division_sysno, receive_address, receive_zip, receive_contact, receive_contact_phone, stock_sysno,
//        payment_type, so_amt, status, order_date, appid, memo) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,now(),?,?)
//从stmt里拿到插入的16个字段名字，组成切片返回
func (executor *prepareInsertExecutor) GetInsertColumns() []string {
	result := make([]string, 0)
	for _, col := range executor.stmt.Columns {
		result = append(result, col.Name.String())
	}
	return result
}
