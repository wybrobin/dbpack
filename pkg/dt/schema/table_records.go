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

package schema

import (
	"fmt"
	"strings"

	"github.com/cectc/dbpack/pkg/mysql"
)

type TableRecords struct {
	TableMeta TableMeta `json:"-"`
	TableName string
	Rows      []*Row
}

type RowLock struct {
	XID      string
	BranchID int64
	RowKey   string
}

func NewTableRecords(meta TableMeta) *TableRecords {
	return &TableRecords{
		TableMeta: meta,
		TableName: meta.TableName,
		Rows:      make([]*Row, 0),
	}
}

func (records *TableRecords) PKFields() []*Field {
	pkRows := make([]*Field, 0)
	pk := records.TableMeta.GetPKName()
	for _, row := range records.Rows {
		for _, field := range row.Fields {
			if strings.EqualFold(field.Name, pk) {
				pkRows = append(pkRows, field)
				break
			}
		}
	}
	return pkRows
}

func BuildLockKey(lockKeyRecords *TableRecords) string {
	if lockKeyRecords == nil || lockKeyRecords.Rows == nil || len(lockKeyRecords.Rows) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(lockKeyRecords.TableName)	//表名
	sb.WriteByte(':')
	fields := lockKeyRecords.PKFields()	//主键列表
	length := len(fields)
	for i, field := range fields {
		switch val := field.Value.(type) {
		case string:
			sb.WriteString(fmt.Sprintf("%s", val))
		case []byte:
			sb.WriteString(fmt.Sprintf("%s", val))
		default:
			sb.WriteString(fmt.Sprintf("%v", val))
		}
		if i < length-1 {
			sb.WriteByte(',')	//多主键用,分隔
		}
	}
	return sb.String()
}
//根据查询结果，解码成 TableRecords 的结构，里面会有多行列名、列值和是否是主键
func BuildBinaryRecords(meta TableMeta, result *mysql.Result) *TableRecords {
	records := NewTableRecords(meta)	//创建一个TableRecords对象
	rs := make([]*Row, 0)	//记录多行数据

	for {
		row, err := result.Rows.Next()	//拿一行
		if err != nil {
			break
		}

		binaryRow := mysql.BinaryRow{Row: row}
		values, err := binaryRow.Decode()	//解码，将一行拆成多列
		if err != nil {
			break
		}
		fields := make([]*Field, 0, len(result.Fields))
		for i, col := range result.Fields {
			field := &Field{
				Name: col.FiledName(),	//列名
				Type: meta.AllColumns[col.FiledName()].DataType,
			}
			if values[i] != nil {	//值不是null，就赋值
				field.Value = values[i].Val
			}
			if strings.EqualFold(col.FiledName(), meta.GetPKName()) {	//如果是主键，则keyType赋值为主键
				field.KeyType = PrimaryKey
			}
			fields = append(fields, field)
		}
		r := &Row{Fields: fields}
		rs = append(rs, r)
	}
	if len(rs) == 0 {
		return nil
	}
	records.Rows = rs
	return records
}

func BuildTextRecords(meta TableMeta, result *mysql.Result) *TableRecords {
	records := NewTableRecords(meta)
	rs := make([]*Row, 0)

	for {
		row, err := result.Rows.Next()
		if err != nil {
			break
		}

		textRow := mysql.TextRow{Row: row}
		values, err := textRow.Decode()
		if err != nil {
			break
		}
		fields := make([]*Field, 0, len(result.Fields))
		for i, col := range result.Fields {
			field := &Field{
				Name: col.FiledName(),
				Type: meta.AllColumns[col.FiledName()].DataType,
			}
			if values[i] != nil {
				field.Value = values[i].Val
			}
			if strings.EqualFold(col.FiledName(), meta.GetPKName()) {
				field.KeyType = PrimaryKey
			}
			fields = append(fields, field)
		}
		r := &Row{Fields: fields}
		rs = append(rs, r)
	}
	if len(rs) == 0 {
		return nil
	}
	records.Rows = rs
	return records
}
