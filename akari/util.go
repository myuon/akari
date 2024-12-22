package akari

import (
	"fmt"
	"slices"
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
	if bytes > 1024*1024*1024 {
		return fmt.Sprintf("%.1f GB", float64(bytes)/1024/1024/1024)
	} else if bytes > 1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(bytes)/1024/1024)
	} else if bytes > 1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	} else {
		return fmt.Sprintf("%d  B", bytes)
	}
}
