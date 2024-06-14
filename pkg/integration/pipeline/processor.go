package pipeline

type ProcessorFunc[T any] func(data []T) ([]T, error)

type ProcessorListNode[T any] struct {
	next *ProcessorListNode[T]
	processor ProcessorFunc[T]
}

type ProcessorList[T any] struct {
	head *ProcessorListNode[T]
	tail *ProcessorListNode[T]
}

func (pl *ProcessorList[T]) AddProcessor(processor ProcessorFunc[T]) {
	if pl.head == nil {
		pl.head = &ProcessorListNode[T]{ nil, processor }
		pl.tail = pl.head
		return
	}

	pl.tail.next = &ProcessorListNode[T]{ nil, processor }
	pl.tail = pl.tail.next
}

func (pln *ProcessorListNode[T]) Process(data []T) ([]T, error) {
	newData, err := pln.processor(data)
	if err != nil {
		return nil, err
	}

	if pln.next != nil {
		return pln.next.Process(newData)
	}

	return newData, nil
}

func (mpl *ProcessorList[T]) Process(data []T) ([]T, error) {
	if mpl.head == nil {
		return data, nil
	}

	return mpl.head.Process(data)
}
