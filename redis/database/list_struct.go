package database

type List struct {
	head *ListNode
	tail *ListNode
	len int
}

type ListNode struct {
	prev *ListNode
	next *ListNode
	val string
}

// 创建有空头结点和空尾结点的链表
func NewList() *List {
	head, tail := &ListNode{}, &ListNode{}
	head.next = tail
	tail.prev = head
	return &List{ head: head, tail: tail, len: 0 }
}

func (l *List) PushHead(val string) {
	if (l == nil) {
		return
	}
	node := &ListNode{ prev: l.head, next: l.head.next, val: val}
	l.head.next = node
	node.next.prev = node
	l.len++
}

func (l *List) PopHead() *ListNode {
	if l == nil || l.len == 0 {
		return nil
	}
	node := l.head.next
	l.head.next = node.next
	node.next.prev = l.head
	l.len--
	return node
}

func (l *List)PushTail(val string) {
	if (l == nil) {
		return
	}
	node := &ListNode{ next: l.tail, prev: l.tail.prev, val: val}
	l.tail.prev = node
	node.prev.next = node
	l.len++
}

func (l *List) PopTail() *ListNode {
	if l == nil || l.len == 0 {
		return nil
	}
	node := l.tail.prev
	l.tail.prev = node.prev
	node.prev.next = l.tail
	l.len--
	return node
}

func (l *List) Len() int {
	if l == nil {
		return -1
	}
	return l.len
}


// 支持负数下标
// 超出范围返回nil
func (l *List) Index(index int) *ListNode {
	if l == nil || l.len == 0 {
		return nil
	}
	if index < 0 {
		index += l.len
		if index < 0 {
			return nil
		}
	} else if index >= l.len {
		return nil
	}

	i := 0
	node := l.head.next
	for i != index {
		i++
		node = node.next
	}
	return node
}

// 若下标值超出范围，则设为边界值
// 若start >= 列表长度，返回空列表
func (l *List) Range(start, stop int) []*ListNode {
	if l == nil ||  l.len == 0{
		return nil
	}

	if start < 0 {
		start += l.len
		if start < 0 {
			start = 0
		}
	} else if start >= l.len {
		return nil
	}
	if stop < 0 {
		stop += l.len
		if stop < 0 {
			return nil
		}
	} else if stop >= l.len {
		stop = l.len - 1
	}

	if start > stop {
		return nil
	}

	nodes := make([]*ListNode, 0, stop-start+1)
	i := 0
	node := l.head.next
	for i < start {
		i++
		node = node.next
	}
	for i <= stop {
		nodes = append(nodes, node)
		i++
		node = node.next
	}
	return nodes
}