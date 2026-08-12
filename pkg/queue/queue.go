// Package queue provides a generic first-in, first-out queue.
package queue

// Queue is a first-in, first-out collection.
type Queue[T any] []T

// Enqueue appends values to the end of the queue.
func (q *Queue[T]) Enqueue(values ...T) {
	*q = append(*q, values...)
}

// Dequeue removes and returns the value at the front of the queue.
func (q *Queue[T]) Dequeue() T {
	value := (*q)[0]
	var zero T
	(*q)[0] = zero
	*q = (*q)[1:]
	return value
}

// Empty reports whether the queue contains no values.
func (q Queue[T]) Empty() bool {
	return len(q) == 0
}
