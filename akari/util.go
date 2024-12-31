package akari

import (
	"fmt"
	"slices"
	"sync"
)

func GetSum[T int | float64](values []T) T {
	total := 0.0
	for _, value := range values {
		total += float64(value)
	}
	return T(total)
}

func GetMean[T int | float64](values []T) T {
	total := 0.0
	for _, value := range values {
		total += float64(value)
	}
	return T(total / float64(len(values)))
}

func GetStddev[T int | float64](values []T) T {
	mean := GetMean(values)
	total := 0.0
	for _, value := range values {
		total += float64((value - mean) * (value - mean))
	}
	return T(total / float64(len(values)))
}

func GetPercentile[T int | float64](values_ []T, percentile int) T {
	values := append([]T{}, values_...)

	slices.Sort(values)

	index := (percentile * len(values)) / 100
	return values[index]
}

func HumanizeBytes(bytes int) string {
	if bytes > 1024*1024*1024*1024 {
		return fmt.Sprintf("%.1f TB", float64(bytes)/1024/1024/1024/1024)
	} else if bytes > 1024*1024*1024 {
		return fmt.Sprintf("%.1f GB", float64(bytes)/1024/1024/1024)
	} else if bytes > 1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(bytes)/1024/1024)
	} else if bytes > 1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	} else {
		return fmt.Sprintf("%.1f B ", float64(bytes))
	}
}

type GlobalVar[T any] struct {
	Value T
	*sync.Mutex
}

func NewGlobalVar[T any](value T) GlobalVar[T] {
	return GlobalVar[T]{Value: value, Mutex: &sync.Mutex{}}
}

func (g *GlobalVar[T]) Store(value T) {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()
	g.Value = value
}

func (g *GlobalVar[T]) Load() T {
	g.Mutex.Lock()
	defer g.Mutex.Unlock()
	return g.Value
}

func InsertAt[T any](slice []T, index int, value T) []T {
	return append(slice[:index], append([]T{value}, slice[index:]...)...)
}

func StringOr(a, b string) string {
	if a == "" {
		return b
	}
	return a
}
