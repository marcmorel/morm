package morm

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

//we use sqlx to manage connection in production. So both connection should never be "not null" at the same time

var dbx *sqlx.DB = nil    //handler to SQLx database connection
var connectionString = "" //will store the DB connection string in case we need it to retry a connection

type fieldStruct struct {
	SQLName string
	Type    reflect.StructField
	Index   int
}

var modelSQLFields map[string]string = nil
var modelFields map[string](*map[string]*fieldStruct)

//DB return pointer to current DB
func DB() *sqlx.DB {
	return dbx
}

//SQLTime prepares a time statement for an sql query
//converts to UTC and then classical format without timezone
func SQLTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

//SafeUint64 convert a sql value to an uint64
func SafeUint64(i interface{}) uint64 {
	if i == nil {
		return 0
	}
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Int32:
		return uint64(i.(int32))
	case reflect.Int64:
		return uint64(i.(int64))
	case reflect.Int16:
		return uint64(i.(int16))
	case reflect.Uint32:
		return uint64(i.(uint32))
	case reflect.Uint64:
		return uint64(i.(uint64))
	case reflect.Uint16:
		return uint64(i.(uint16))
	case reflect.Slice:
		r, err := strconv.ParseUint(string(i.([]byte)), 10, 64)
		if err != nil {
			return 0
		}
		return r
	}
	panic("Safeuint64 unsupported type : " + v.Kind().String())
}

//SafeInt64 convert a sql value to an int64
func SafeInt64(i interface{}) int64 {
	if i == nil {
		return 0
	}
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Int32:
		return int64(i.(int32))
	case reflect.Int64:
		return int64(i.(int64))
	case reflect.Int16:
		return int64(i.(int16))
	case reflect.Uint32:
		return int64(i.(uint32))
	case reflect.Uint64:
		return int64(i.(uint64))
	case reflect.Uint16:
		return int64(i.(uint16))
	case reflect.Slice:
		r, err := strconv.ParseInt(string(i.([]byte)), 10, 64)
		if err != nil {
			return 0
		}
		return r
	}
	panic("Safeint64 unsupported type : " + v.Kind().String())
}

//SafeInt convert a sql value to an int
func SafeInt(i interface{}) int {
	if i == nil {
		return 0
	}
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Int64:
		return int(i.(int64))
	case reflect.Int32:
		return int(i.(int32))
	case reflect.Int16:
		return int(i.(int16))
	case reflect.Uint32:
		return int(i.(uint32))
	case reflect.Uint16:
		return int(i.(uint16))
	case reflect.Slice:
		r, err := strconv.ParseInt(string(i.([]byte)), 10, 32)
		if err != nil {
			return 0
		}
		return int(r)
	}
	panic("Safeint64 unsupported type : " + v.Kind().String())
}

//SafeFloat32 convert a sql value to an int
func SafeFloat32(i interface{}) float32 {
	if i == nil {
		return 0
	}
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Int32:
		return float32(i.(int32))
	case reflect.Int16:
		return float32(i.(int16))
	case reflect.Uint32:
		return float32(i.(uint32))
	case reflect.Uint16:
		return float32(i.(uint16))
	case reflect.Float32:
		return float32(i.(float32))
	case reflect.Slice:
		r, err := strconv.ParseFloat(string(i.([]byte)), 32)
		if err != nil {
			return 0
		}
		return float32(r)
	}
	panic("Safeint64 unsupported type : " + v.Kind().String())
}

//SafeString convert a sql value to a string
func SafeString(i interface{}) string {
	if i == nil {
		return ""
	}
	var str sql.NullString
	if str.Scan(i) != nil {
		return ""
	}
	return str.String
}

/*CheckNilTime check if time is nil. if it is, returs now() shifted by year, month, day. Otherwise returns the input value */
func CheckNilTime(t *time.Time, year int, month int, day int) *time.Time {
	if t != nil {
		return t
	}
	newt := time.Now().AddDate(year, month, day)
	return &newt
}

/*SafeTime converts an interface{} to time pointer*/
func SafeTime(i interface{}) *time.Time {

	if i == nil {
		return nil
	}
	t := i.(time.Time)
	return &t
}

/*RowScan takes a mapper function and applies it to the row*/
func RowScan(rows *sqlx.Rows, fn func(map[string]interface{}) interface{}) (interface{}, error) {
	m := make(map[string]interface{})
	err := rows.MapScan(m)
	if err != nil {
		return nil, err
	}
	ev := fn(m)
	return ev, nil
}

//CheckDB will establish a connection if none exists and ping it if not. Will try once
//reestablish a connection
func CheckDB() (*sqlx.DB, error) {
	if dbx == nil {
		return nil, errors.New("DB not initialized")
	}
	err := dbx.Ping()
	if err != nil {
		_, err2 := connectDB()
		if err2 != nil {
			return nil, err2
		}
		//still here ? second DB connection was successful

	}
	return dbx, nil
}

// InitDB initiates the connection to the DB through the SQLX middleware and returns an error; no panic
// driver is supposed to be mysql and won't be modified
func InitDB(datasource string) (*sqlx.DB, error) {

	connectionString = strings.Trim(datasource, " ") + "?charset=utf8&parseTime=True&loc=Local"
	return connectDB()
}

//connectDB triggers the actual DB connection.
func connectDB() (*sqlx.DB, error) {
	var err error
	dbx, err = sqlx.Open("mysql", connectionString)
	if err != nil {
		return nil, err
	}
	dbx.Mapper = reflectx.NewMapperFunc("sqlx", strings.ToLower)

	return dbx, err
}

// GetDB will return a pointer to the SLQX conneciton pool or an error if it hasn't been initialized yet
func GetDB() (*sqlx.DB, error) {
	if dbx == nil {
		return nil, errors.New("DB connection pool hasn't been initialzed yet")
	}
	return dbx, nil
}

func parseTag(tags reflect.StructTag) string {
	return tags.Get("sqlx")
}

/*InitModels will prepare a map of model fields to speed up things later on*/
func InitModels(mods []interface{}) {
	for _, m := range mods {
		prepareModelForScan(m)
	}
}

type hasTableName interface {
	TableName() string
}

func prepareModelForScan(m interface{}) {
	if modelSQLFields == nil {
		modelSQLFields = make(map[string]string)
	}

	if modelFields == nil {
		modelFields = make(map[string](*map[string]*fieldStruct))
	}

	s := reflect.ValueOf(m).Elem()
	numFields := s.NumField()
	thismodelFields := make(map[string]*fieldStruct)
	typeOfT := s.Type()
	tableName := ""
	if mInstance, ok := reflect.New(typeOfT).Interface().(hasTableName); ok {
		tableName = mInstance.TableName()
	} else {
		panic("PreparemodelForScan has to be called on objects implementing TableName()")
	}

	for i := 0; i < numFields; i++ {
		if tag := parseTag(typeOfT.Field(i).Tag); len(tag) > 0 {
			thismodelFields[typeOfT.Field(i).Name] = &fieldStruct{SQLName: tag, Type: typeOfT.Field(i), Index: i}
		}
	}
	modelFields[tableName] = &thismodelFields

}

func getChainedFields(qualifiedName string) string {
	rawsql := ""
	rawsqlLast := ""
	var prefix string
	var tableName string

	path := strings.Split(qualifiedName, ".")
	if len(path) == 1 {
		prefix = qualifiedName + "."
		tableName = qualifiedName
	} else {
		prefix = strings.Join(path[1:], ".") + "."
		tableName = path[len(path)-1]
	}

	for _, v := range *modelFields[tableName] {
		rawsql = rawsqlLast
		rawsql += tableName + "." + v.SQLName + " \"" + prefix + v.SQLName + "\""
		rawsqlLast = rawsql + ",\n"
	}
	return rawsql
}

/*GetSQLFields creates a list of field for an sql select query like tableA.field1 `tA.f1`, `tA.f2`, `tB.f1`
 */
func GetSQLFields(tableNames ...string) string {
	result := ""
	oldResult := ""
	for _, qualifiedName := range tableNames {
		//check in cache:
		temp, ok := modelSQLFields[qualifiedName]
		if !ok { //cache missed
			temp = getChainedFields(qualifiedName)
			modelSQLFields[qualifiedName] = temp // cache it for next time
		}
		result += temp
		oldResult = result
		result += ",\n"
	}
	return oldResult
}

/*GetMapValues create a map of fields name -> fields values from a struct using the reflect package
 */
func GetMapValues(m interface{}) *map[string](string) {
	tableName := ""
	result := make(map[string](string))
	if mInstance, ok := m.(hasTableName); ok {
		tableName = mInstance.TableName()
	} else {
		panic("GetMapValues has to be called on objects implementing TableName()")
	}

	fields, ok := modelFields[tableName]
	if !ok {
		prepareModelForScan(m)
		fields = modelFields[tableName]
	}
	val := reflect.ValueOf(m).Elem()
	for _, field := range *fields {
		valuefield := val.Field(field.Index)
		f := valuefield.Interface()
		v := reflect.ValueOf(f)
		stringvalue := ""
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			stringvalue = strconv.FormatInt(v.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			stringvalue = strconv.FormatUint(v.Uint(), 10)
		case reflect.Float32:
			stringvalue = strconv.FormatFloat(v.Float(), 'f', 2, 32)
		case reflect.Float64:
			stringvalue = strconv.FormatFloat(v.Float(), 'f', 2, 64)
		case reflect.String:
			stringvalue = "\"" + MysqlRealEscapeString(v.String()) + "\""
		case reflect.Ptr: //*time.Time value probably
			if v.IsNil() {
				stringvalue = "null"
			} else {
				if v.Elem().Kind() == reflect.Struct {
					stringvalue = "\"" + SQLTime(v.Elem().Interface().(time.Time)) + "\""
				}
			}
		}
		result[field.SQLName] = stringvalue
	}
	return &result

}

/*GetSQLValues create string of fields name -> fields values from a struct using the reflect package
 */
func GetSQLValues(m interface{}) string {

	mp := GetMapValues(m)
	result := ""
	oldResult := ""
	for _, value := range *mp {
		result += value
		oldResult = result
		result += ", "
	}
	return oldResult
}

/*GetSQLFieldsAndValues create string of fields name and a string of fields values from a struct using the reflect package
 */
func GetSQLFieldsAndValues(m interface{}) (string, string) {

	mp := GetMapValues(m)
	resultV := ""
	oldResultV := ""
	resultK := ""
	oldResultK := ""
	for key, value := range *mp {
		resultV += value
		resultK += "`" + key + "`"
		oldResultV = resultV
		oldResultK = resultK
		resultV += ", "
		resultK += ", "
	}
	return oldResultK, oldResultV
}

/*Model is the interface used for this light orm*/
type Model interface {
	TableName() string
	SetID(uint64)
	GetID() uint64
	Save() error
	Update() error
}

func setTimeField(m Model, field string, time *time.Time) {
	reflect.ValueOf(m).Elem().FieldByName(field).Set(reflect.ValueOf(time))
}

/*Save will save the corresponding model to db and throw an error if it fails
if transaction != nil, will use it. Otherwise use classic db*/
func Save(m Model, transaction *sqlx.Tx) error {
	if m.GetID() != 0 {
		return Update(m, transaction)
	}
	now := time.Now()
	setTimeField(m, "CreatedAt", &now)
	setTimeField(m, "UpdatedAt", &now)
	fields, values := GetSQLFieldsAndValues(m)
	query := `insert into ` + m.TableName() + ` ( ` + fields + `) VALUES (` + values + `)`
	if transaction != nil {
		result, err := transaction.Exec(query)
		if err != nil {
			return err
		}
		if id, err := result.LastInsertId(); err == nil {
			m.SetID(uint64(id))
			return nil
		}
		return nil
	}
	result, err := dbx.Exec(query)
	if err != nil {
		return err
	}

	if id, err := result.LastInsertId(); err == nil {
		m.SetID(uint64(id))
		return nil
	}
	return err

}

func prepareUpdateQuery(m Model) (string, error) {

	if m.GetID() == 0 {
		return "", errors.New("ID is null; update impossible ")
	}
	now := time.Now()
	setTimeField(m, "UpdatedAt", &now)
	mp := GetMapValues(m)
	sqlvals := ""
	oldSqlvals := ""
	sqlid := ""
	for key, val := range *mp {
		if key == "id" {
			sqlid = val
		} else {
			sqlvals += key + "=" + val
			oldSqlvals = sqlvals
			sqlvals += ", "
		}
	}

	query := `update ` + m.TableName() + ` set ` + oldSqlvals + ` where id=` + sqlid + `;`
	return query, nil

}

/*Update will set the updated_at field at now() and save to DB. will return an error if model.id = 0 */
func Update(m Model, transaction *sqlx.Tx) error {
	query, err := prepareUpdateQuery(m)
	if err != nil {
		return err
	}
	if transaction != nil {
		if _, err := transaction.Exec(query); err != nil {
			return err
		}
		return nil
	}
	if _, err := dbx.Exec(query); err != nil {
		return err
	}
	return nil
}

/*UpdateDebug will set the updated_at field at now() and save to DB. will return an error if model.id = 0. Will print the request */
func UpdateDebug(m Model, transaction *sqlx.Tx) error {
	query, err := prepareUpdateQuery(m)
	fmt.Printf("Update Debug, request executed :%s\n", query)
	if err != nil {
		return err
	}
	if transaction != nil {
		if _, err := transaction.Exec(query); err != nil {
			return err
		}
		return nil
	}
	if _, err := dbx.Exec(query); err != nil {
		return err
	}
	return nil
}

/*MysqlRealEscapeString sanitize sql strings*/
func MysqlRealEscapeString(value string) string {
	replace := [][2]string{
		[2]string{"\\", "\\\\"},
		[2]string{"'", `\'`},
		[2]string{"\\0", "\\\\0"},
		[2]string{"\n", "\\n"},
		[2]string{"\r", "\\r"},
		[2]string{`"`, `\"`},
		[2]string{"\x1a", "\\Z"}}
	for _, s := range replace {
		value = strings.Replace(value, s[0], s[1], -1)
	}
	return value
}

/*FindByColumn will build a query without joint and with an WHERE statement built using
AND operators for all fields provided.
WILL add a tablename.deleted_at is null to avoid finding soft deleted records
FindByColmun will return a map[string](interface{}) to be fed to your CreateXXXFromMap function where XXX is your model type
*/
func FindByColumn(tablename string, m map[string](string)) (map[string](interface{}), error) {
	query := `select ` + GetSQLFields(tablename) + ` from ` + tablename + ` where `

	for k, v := range m {
		query += tablename + "." + k + "=" + v
		query += " AND "
	}
	query += tablename + ".deleted_at is null limit 1"

	r := make(map[string]interface{})
	if err := dbx.QueryRowx(query).MapScan(r); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return r, nil
}

/*FindAllByColumn will build a query without joint and with an WHERE statement built using
AND operators for all fields provided.
WILL add a tablename.deleted_at is null to avoid finding soft deleted records

FindAllByColmun will return a  []map[string](interface{}) to be fed x times to your CreateXXXFromMap function where XXX is your model type
*/
func FindAllByColumn(tablename string, m map[string](string)) ([]map[string](interface{}), error) {
	query := `select ` + GetSQLFields(tablename) + ` from ` + tablename + ` where `

	for k, v := range m {
		query += tablename + "." + k + "=" + v
		query += " AND "
	}
	query += tablename + ".deleted_at is null"
	result := make([]map[string]interface{}, 0, 100)
	rows, err := dbx.Queryx(query)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		r := make(map[string]interface{})
		if err := rows.MapScan(r); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		result = append(result, r)
	}
	return result, nil
}

//ModelDelete delete the model in db
func ModelDelete(tablename string, ID uint64) error {
	query := `delete from ` + tablename + ` where id=` + strconv.FormatUint(ID, 10) + `;`
	_, err := dbx.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
