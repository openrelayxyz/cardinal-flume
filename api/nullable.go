package api

import (
	"database/sql/driver"
)

func convertAssign[T any](out *T, in any) error {
	*out = in.(T)
	return nil
}

type nullable[T any] struct {
	Actual T
	Valid  bool
}

func (n *nullable[T]) Scan(value any) error {
	n.Valid = false
	if value == nil {
		var v T
		n.Actual = v
		return nil
	}
	err := convertAssign[T](&n.Actual, value)
	if err == nil {
		n.Valid = true
	}
	return err
}

func (n nullable[T]) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Actual, nil
}