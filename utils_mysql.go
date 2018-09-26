package db2struct_gorm

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type ColumnsInfo struct {
	ColumnName string // COLUMN_NAME 行名称
	ColumnType string // COLUMN_TYPE
	ColumnKey  string // COLUMN_KEY
	Extra      string // EXTRA
	DataType   string // DATA_TYPE 类型
	NullAble   string // IS_NULLABLE 是否为空
	Default    string // COLUMN_DEFAULT 默认值
	Comment    string // COLUMN_COMMENT 备注信息
}

// GetColumnsFromMysqlTable Select column details from information schema and return map of map
func GetColumnsFromMysqlTable(mariadbUser string, mariadbPassword string, mariadbHost string, mariadbPort int, mariadbDatabase string, mariadbTable string) ([]ColumnsInfo, error) {

	var err error
	var db *sql.DB
	if mariadbPassword != "" {
		db, err = sql.Open("mysql", mariadbUser+":"+mariadbPassword+"@tcp("+mariadbHost+":"+strconv.Itoa(mariadbPort)+")/"+mariadbDatabase+"?&parseTime=True")
	} else {
		db, err = sql.Open("mysql", mariadbUser+"@tcp("+mariadbHost+":"+strconv.Itoa(mariadbPort)+")/"+mariadbDatabase+"?&parseTime=True")
	}
	defer db.Close()

	// Check for error in db, note this does not check connectivity but does check uri
	if err != nil {
		fmt.Println("Error opening mysql db: " + err.Error())
		return nil, err
	}

	// Store colum as map of maps
	columnDataTypes := make([]ColumnsInfo, 0)
	// Select columnd data from INFORMATION_SCHEMA
	columnDataTypeQuery := "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA, DATA_TYPE, IS_NULLABLE , COLUMN_DEFAULT, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND table_name = ?"

	if Debug {
		fmt.Println("running: " + columnDataTypeQuery)
	}

	rows, err := db.Query(columnDataTypeQuery, mariadbDatabase, mariadbTable)

	if err != nil {
		fmt.Println("Error selecting from db: " + err.Error())
		return nil, err
	}
	if rows != nil {
		defer rows.Close()
	} else {
		return nil, errors.New("No results returned for table")
	}

	for rows.Next() {
		var columnData = ColumnsInfo{}
		var i interface{}
		rows.Scan(&columnData.ColumnName, &columnData.ColumnType, &columnData.ColumnKey,
			&columnData.Extra, &columnData.DataType, &columnData.NullAble, &i,
			&columnData.Comment)
		if i == nil {
			columnData.Default = "null"
		} else {
			d, ok := i.([]byte)
			if ok {
				columnData.Default = string(d)
			}
		}

		columnDataTypes = append(columnDataTypes, columnData)
	}

	return columnDataTypes, err
}

// Generate go struct entries for a map[string]interface{} structure
func generateMysqlTypes(objs []ColumnsInfo, depth int, jsonAnnotation bool, gormAnnotation bool, gureguTypes bool) string {
	structure := "struct {"

	for _, col := range objs {
		nullable := false
		if ignoreCaseEq(col.NullAble, "YES") {
			nullable = true
		}

		// Get the corresponding go value type for this mysql type
		var valueType string
		// If the guregu (https://github.com/guregu/null) CLI option is passed use its types, otherwise use go's sql.NullX

		valueType = mysqlTypeToGoType(col.DataType, nullable, gureguTypes)

		fieldName := fmtFieldName(stringifyFirstChar(col.ColumnName))
		var annotations []string
		if gormAnnotation == true {
			at := "gorm:\"column:" + col.ColumnName
			if ignoreCaseEq(col.ColumnKey, "PRI") {
				at += ";primary_key"
			}

			if ignoreCaseEq(col.Extra, "auto_increment") {
				at += ";AUTO_INCREMENT"
			}
			at += fmt.Sprintf(";default:'%s'", col.Default)
			at += "\""
			annotations = append(annotations, at)
		}
		if jsonAnnotation == true {
			annotations = append(annotations, fmt.Sprintf("json:\"%s\"", col.ColumnName))
		}
		if len(annotations) > 0 {
			structure += fmt.Sprintf("\n%s %s `%s` // %s",
				fieldName,
				valueType,
				strings.Join(annotations, " "),
				col.Comment)

		} else {
			structure += fmt.Sprintf("\n%s %s // %s",
				fieldName,
				valueType,
				col.Comment)
		}
	}
	return structure
}

// mysqlTypeToGoType converts the mysql types to go compatible sql.Nullable (https://golang.org/pkg/database/sql/) types
func mysqlTypeToGoType(mysqlType string, nullable bool, gureguTypes bool) string {
	switch mysqlType {
	case "tinyint", "int", "smallint", "mediumint":
		if nullable {
			if gureguTypes {
				return gureguNullInt
			}
			return sqlNullInt
		}
		return golangInt
	case "bigint":
		if nullable {
			if gureguTypes {
				return gureguNullInt
			}
			return sqlNullInt
		}
		return golangInt64
	case "char", "enum", "varchar", "longtext", "mediumtext", "text", "tinytext":
		if nullable {
			if gureguTypes {
				return gureguNullString
			}
			return sqlNullString
		}
		return "string"
	case "date", "datetime", "time", "timestamp":
		if nullable && gureguTypes {
			return gureguNullTime
		}
		return golangTime
	case "decimal", "double":
		if nullable {
			if gureguTypes {
				return gureguNullFloat
			}
			return sqlNullFloat
		}
		return golangFloat64
	case "float":
		if nullable {
			if gureguTypes {
				return gureguNullFloat
			}
			return sqlNullFloat
		}
		return golangFloat32
	case "binary", "blob", "longblob", "mediumblob", "varbinary":
		return golangByteArray
	}
	return ""
}
