package main

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Model struct {
	table        string
	table_prefix string
	db           *sql.DB
	field        []string
	db_config    map[string]string
	where        string
	conditions   []interface{}
}

func (model *Model) Connect(db_index int, config []map[string]string) {
	model.db_config = config[db_index]
	model.table_prefix = model.db_config["table_prefix"]
	dsn := model.db_config["username"] + ":" + model.db_config["password"] + "@tcp(" + model.db_config["host"] + ":" + model.db_config["port"] + ")/" + model.db_config["dbname"]
	fmt.Println(dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
		return
	}
	err = db.Ping()
	if err != nil {
		panic(err)
		return
	}
	model.db = db
}

/*
设置查询字段
field "id,name"
*/
func (model *Model) Field(field []string) *Model {
	model.field = field
	return model
}

/*
设置表
table "指定表，无前缀"
*/
func (model *Model) Table(table string) string {
	if len(table) > 0 {
		return "`" + model.db_config["dbname"] + "`.`" + table + "`"
	} else {
		return "`" + model.db_config["dbname"] + "`.`" + model.db_config["table_prefix"] + "_" + model.table + "`"
	}

}

/*
设置查询条件
where:="and id=? and username=?"
condition := []interface{}{0,"bidcms"}
返回添加的行数最后id,有主键的情况下
*/
func (model *Model) Where(where string, conditions []interface{}) *Model {
	model.where = where
	model.conditions = conditions
	return model
}

/*
获取总数
*/
func (model *Model) GetCount(table string) int64 {
	sqlStr := "select count(*) as total from " + model.Table(table) + " where " + model.where
	stmt, err := model.db.Prepare(sqlStr)
	defer stmt.Close()
	if err != nil {
		panic(err)
		return 0
	}
	var total int64
	err = stmt.QueryRow(model.conditions...).Scan(&total)
	if err != nil {
		panic(err.Error())
		return 0
	}
	model.where = ""
	return total
}

/*
查询多行数据
table "指定表，要带前缀"
返回添加的行数最后id,有主键的情况下
*/
func (model *Model) GetPage(showpage map[string]string, order string, table string) (map[interface{}]map[string]interface{}, string) {
	var field string
	if len(model.field) < 1 {
		field = "*"
	} else {
		field = "`" + strings.Join(model.field, "`,`") + "`"
	}
	if len(model.where) == 0 {
		model.where = "1"
	}
	sqlStr := "select " + field + " from " + model.Table(table) + " where " + model.where
	if len(order) > 0 {
		sqlStr += " order by " + order
	}
	if showpage["is_show"] == "1" {
		count := model.GetCount(table)
		pageSize, err := strconv.ParseInt(showpage["page_size"], 10, 64)
		if err != nil {
			pageSize = 10
		}
		page, err := strconv.ParseInt(showpage["page"], 10, 64)
		if err != nil {
			page = 0
		}
		total := math.Ceil(float64(count) / float64(pageSize))
		fmt.Println(total) //分页页数
		sqlStr += " limit " + strconv.FormatInt(page*pageSize, 10) + "," + showpage["page_size"]
	}
	stmt, err := model.db.Prepare(sqlStr)
	defer stmt.Close()
	if err != nil {
		panic(err)
		return nil, sqlStr
	}
	var rows *sql.Rows
	if len(model.conditions) > 0 {
		rows, err = stmt.Query(model.conditions...)
		if err != nil {
			panic(err.Error())
			return nil, sqlStr
		}
	} else {
		rows, err = stmt.Query()
	}
	var cols []string
	if field == "*" {
		cols, _ = rows.Columns()
	} else {
		cols = model.field
	}
	values := make([]string, len(cols))
	scans := make([]interface{}, len(values))
	for i := range values {
		scans[i] = &values[i]
	}
	results := make(map[interface{}]map[string]interface{})
	var index = 0
	for rows.Next() {
		rows.Scan(scans...)
		row := make(map[string]interface{}) //每行数据
		for k, v := range values {          //每行数据是放在values里面，现在把它挪到row里
			key := cols[k]
			row[key] = v
		}
		if len(showpage["index"]) > 0 && row[showpage["index"]] != nil {
			results[row[showpage["index"]]] = row
		} else {
			results[index] = row
			index++
		}

	}

	model.where = ""

	model.field = []string{}
	return results, sqlStr
}

/*
更新多行数据
data map[string]interface{}{"username": "lim", "sort": 23}
table "指定表，要带前缀"
返回影响的行数
*/
func (model *Model) Update(data map[string]interface{}, table string) (int64, string) {
	if len(model.where) == 0 {
		return 0, ""
	}
	var fields []string
	for i := range data {
		fields = append(fields, i+"=?")
	}
	sqlStr := "update " + model.Table(table) + " set " + strings.Join(fields, ",") + "  where " + model.where
	var values []interface{}
	for _, value := range data {
		values = append(values, value)
	}
	for _, value := range model.conditions {
		values = append(values, value)
	}
	stmt, err := model.db.Prepare(sqlStr)
	if err != nil {
		panic(err.Error())
		return 0, sqlStr
	}
	var result sql.Result
	result, err = stmt.Exec(values...)
	if err != nil {
		panic(err.Error())
		return 0, sqlStr
	}
	rows, err := result.RowsAffected()
	if err != nil {
		panic(err.Error())
		return 0, sqlStr
	}
	model.where = ""

	return rows, sqlStr
}

/*
删除多行数据
table "指定表，要带前缀"
返回影响的行数
*/
func (model *Model) Delete(table string) (int64, string) {
	if len(model.where) == 0 {
		return 0, ""
	}
	sqlStr := "delete from " + model.Table(table) + "  where " + model.where

	stmt, err := model.db.Prepare(sqlStr)
	if err != nil {
		panic(err.Error())
		return 0, sqlStr
	}
	var result sql.Result
	result, err = stmt.Exec(model.conditions...)
	if err != nil {
		panic(err.Error())
		return 0, sqlStr
	}
	rows, err := result.RowsAffected()
	if err != nil {
		panic(err.Error())
		return 0, sqlStr
	}
	model.where = ""

	return rows, sqlStr
}

/*
批量添加数据
data []map[string]interface{}{{"username": "lim", "sort": 23, "password": "male"}}
返回添加的行数最后id,有主键的情况下
*/
func (model *Model) Insert(data []map[string]interface{}, table string) (int64, string) {
	if len(data) == 0 {
		return 0, ""
	}
	var fields []string
	var placeholder []string
	fmt.Println(data[0])
	for i := range data[0] {
		fields = append(fields, i)
		placeholder = append(placeholder, "?")
	}
	sqlStr := "insert into " + model.Table(table) + "(`" + strings.Join(fields, "`,`") + "`) values(" + strings.Join(placeholder, ",") + ")"
	values := []interface{}{}
	for index := range data {
		for _, value := range data[index] {
			values = append(values, value)
		}
		if index > 0 {
			sqlStr += ",(" + strings.Join(placeholder, ",") + ")"
		}
	}
	stmt, err := model.db.Prepare(sqlStr)
	if err != nil {
		panic(err.Error())
		return 0, sqlStr
	}
	var result sql.Result
	result, err = stmt.Exec(values...)
	if err != nil {
		panic(err.Error())
		return 0, sqlStr
	}
	rows, err := result.LastInsertId()
	if err != nil {
		panic(err.Error())
		return 0, sqlStr
	}

	return rows, sqlStr
}

func main() {
	model := new(Model)
	c := []map[string]string{{"username": "root", "password": "root123", "host": "127.0.0.1", "port": "3306", "dbname": "crm_local", "table_prefix": "oa"}}

	model.Connect(0, c)
	//condition := []interface{}{10000}
	model.table = "system_sms"
	//insertId,sql := model.Insert([]map[string]interface{}{{"username": "lim", "sort": 23, "password": "male"}}, sqlStr)
	//fmt.Println(insertId)
	//data := map[string]interface{}{"contact_qq": "2559009123"}
	//res,sql := model.Update("id=?", condition, data, sqlStr)
	//fmt.Println(res)

	drows, sql := model.Where("Id>?", []interface{}{0}).GetPage(map[string]string{"index": "Id", "is_show": "1", "page": "0", "page_size": "1"}, "id desc", "sms_send")
	fmt.Println(drows)
	fmt.Println(sql)
	drows, sql = model.Where("id>?", []interface{}{0}).Field([]string{"id", "agent_id", "msg_id"}).GetPage(map[string]string{"index": "id", "is_show": "1", "page": "0", "page_size": "1"}, "id desc", "")
	fmt.Println(drows)
	fmt.Println(sql)
	model.db.Close()
}
