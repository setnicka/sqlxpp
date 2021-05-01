package sqlxpp

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

// dbFields returns list of names of fields with the db:name tag
func dbFields(values interface{}, excludeFields []string) []string {
	excludeFieldsMap := map[string]bool{}
	for _, excludeField := range excludeFields {
		excludeFieldsMap[excludeField] = true
	}

	v := reflect.ValueOf(values)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	fields := []string{}
	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			dbName := field.Tag.Get("db")
			_, exclude := excludeFieldsMap[dbName]
			if dbName != "" && dbName != "-" && !exclude {
				fields = append(fields, dbName)
			}
			// for embedded structs
			if field.Type.Kind() == reflect.Struct {
				fields = append(fields, dbFields(v.Field(i).Interface(), excludeFields)...)
			}
		}
		return fields
	}
	if v.Kind() == reflect.Map {
		for _, keyv := range v.MapKeys() {
			fields = append(fields, keyv.String())
		}
		return fields
	}
	panic(fmt.Errorf("DBFields requires a struct or a map, found: %s", v.Kind().String()))
}

func genUpdateString(fields []string) string {
	updateFields := make([]string, 0, len(fields))
	for _, f := range fields {
		updateFields = append(updateFields, fmt.Sprintf("\"%s\"=:%s", f, f))
	}
	return strings.Join(updateFields, ", ")
}

// IsNotFoundError checks if the error means "no DB error but record not found"
func IsNotFoundError(err error) bool {
	return errors.Cause(err) == sql.ErrNoRows
}
