package akari

import "slices"

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
