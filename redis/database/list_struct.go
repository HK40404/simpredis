package database

import (
	"bytes"
	"container/list"
)

var PAGESIZE = 1024

// 使用快速列表，拥有更好的range,find,add效率和紧凑的内存存储
type QuickList struct {
	l   *list.List
	len int
}

// offset为-1、ele为第一个元素时，表示到了链表左边的尽头
// offset为len(page)、ele为最后一个元素时，表示到了链表右边的尽头
type iterator struct {
	ele    *list.Element
	offset int
	ql     *QuickList
}

func (it *iterator) get() any {
	page := it.ele.Value.([]any)
	return page[it.offset]
}

func (it *iterator) page() []any {
	return it.ele.Value.([]any)
}

func (it *iterator) next() bool {
	page := it.page()
	if it.offset < len(page)-1 {
		it.offset++
		return true
	}
	if it.ele != it.ql.l.Back() {
		it.ele = it.ele.Next()
		it.offset = 0
		return true
	}
	// it指向了最后一个元素
	it.offset = len(page)
	return false
}

func (it *iterator) prev() bool {
	if it.offset == 0 {
		if it.ele == it.ql.l.Front() {
			// 当前已经是第一个元素
			it.offset = -1
			return false
		} else {
			it.ele = it.ele.Prev()
			it.offset = len(it.page()) - 1
			return true
		}
	} else {
		it.offset--
		return true
	}
}

func (it *iterator) atEnd() bool {
	if it.ql.l.Len() == 0 {
		return true
	}
	if it.ele != it.ql.l.Back() {
		return false
	}
	return it.offset == len(it.page())
}

func (it *iterator) atBegin() bool {
	if it.ql.l.Len() == 0 {
		return true
	}
	if it.ele != it.ql.l.Front() {
		return false
	}
	return it.offset == -1
}

// 需要注意的情况：
// 1. 去掉元素后，页面为空，则需要回收，并指向下一个页面开头
// 2. 元素为该页面最后一个元素，需要指向下一个页面开头
// 3. 元素为整个链表最后一个元素，iter不需要变
func (iter *iterator) remove() any {
	page := iter.page()
	value := page[iter.offset]
	page = append(page[:iter.offset], page[iter.offset+1:]...)
	iter.ql.len--
	if len(page) > 0 {
		iter.ele.Value = page
		if iter.offset == len(page) {
			if !iter.atEnd() {
				iter.ele = iter.ele.Next()
				iter.offset = 0
			}
		}
	} else {
		if iter.ele != iter.ql.l.Back() {
			nextnode := iter.ele.Next()
			iter.ql.l.Remove(iter.ele)
			// 若iter指向最后一个，ele会变为空
			iter.ele = nextnode
			iter.offset = 0
		} else {
			// 尾页面被删除
			if prevnode := iter.ele.Prev(); prevnode != nil {
				// 移动到atEnd() == true的位置
				iter.ql.l.Remove(iter.ele)
				iter.ele = prevnode
				iter.offset = 0
			} else {
				// 链表已经空
				iter.ql.l.Remove(iter.ele)
				iter.ele = nil
				iter.offset = -1
			}
		}
	}
	return value
}

// 创建有空头结点和空尾结点的链表
func NewQuickList() *QuickList {
	return &QuickList{l: list.New()}
}

func (ql *QuickList) Len() int {
	return ql.len
}

func (ql *QuickList) First() *iterator {
	if ql.len == 0 {
		return nil
	}
	return &iterator{
		ele:    ql.l.Front(),
		offset: 0,
		ql:     ql,
	}
}

func (ql *QuickList) PushBack(v any) {
	ql.len++
	if ql.l.Len() == 0 {
		page := make([]any, 0, PAGESIZE)
		page = append(page, v)
		ql.l.PushBack(page)
		return
	}

	backnode := ql.l.Back()
	backpage := backnode.Value.([]any)
	// 最后一页满了
	if len(backpage) == cap(backpage) {
		page := make([]any, 0, PAGESIZE)
		page = append(page, v)
		ql.l.PushBack(page)
		return
	}

	backpage = append(backpage, v)
	backnode.Value = backpage
}

func (ql *QuickList) Find(index int) *iterator {
	if index < 0 || index >= ql.len {
		return nil
	}

	// 若要找的元素在链表前半部分，从前面开始找
	if index < ql.len/2 {
		i := 0
		node := ql.l.Front()
		page := node.Value.([]any)
		// i每次移动到后一页的第一个位置
		for i+len(page) <= index {
			i += len(page)
			node = node.Next()
			page = node.Value.([]any)
		}
		offset := index - i
		return &iterator{
			ele:    node,
			offset: offset,
			ql:     ql,
		}
	} else {
		i := ql.len - 1
		node := ql.l.Back()
		page := node.Value.([]any)
		// i每次移动到前一页的最后一个位置
		for i-len(page) >= index {
			i -= len(page)
			node = node.Prev()
			page = node.Value.([]any)
		}
		offset := index - (i - len(page)) - 1
		return &iterator{
			ele:    node,
			offset: offset,
			ql:     ql,
		}
	}
}

func (ql *QuickList) Insert(index int, val any) {
	if index < 0 || index > ql.len {
		return
	}

	if index == ql.len {
		ql.PushBack(val)
		return
	}

	// 最后再加，不然会影响find的判断
	defer func() { ql.len++ }()

	iter := ql.Find(index)
	page := iter.page()
	offset := iter.offset
	if len(page) < cap(page) {
		page = append(page[:offset+1], page[offset:]...)
		page[offset] = val
		iter.ele.Value = page
		return
	}

	// 要插入的页面已经满了
	// 将原来页面一分为二，变成两个新页面，每个页面有原来一半元素
	newpage := make([]any, 0, PAGESIZE)
	if offset < PAGESIZE/2 {
		newpage = append(newpage, page[PAGESIZE/2:]...)
		page = append(page[:offset+1], page[offset:PAGESIZE/2]...)
		page[offset] = val
	} else {
		newpage = append(newpage, page[PAGESIZE/2:offset+1]...)
		newpage[offset] = val
		newpage = append(newpage, page[offset:]...)
		page = page[:PAGESIZE/2]
	}
	iter.ele.Value = page
	ql.l.InsertAfter(newpage, iter.ele)
}

func (ql *QuickList) ForEach(f func(int, any) bool) {
	iter := ql.First()
	index := 0
	value := iter.get()
	for {
		if !f(index, value) {
			break
		}
		if !iter.next() {
			break
		}
		value = iter.get()
		index++
	}
}

func (ql *QuickList) RemoveByCount(val []byte, count int) int {
	if count == 0 {
		return ql.removeAll(val)
	}
	if count > 0 {
		return ql.removeCount(val, count)
	}
	if count < 0 {
		return ql.removeCountReverse(val, -count)
	}
	return -1
}

func (ql *QuickList) removeAll(val []byte) int {
	if ql.Len() == 0 {
		return 0
	}

	iter := ql.Find(0)
	count := 0
	for !iter.atEnd() {
		v := iter.get().([]byte)
		if bytes.Equal(v, val) {
			iter.remove()
			count++
		} else {
			iter.next()
		}
	}
	return count
}

func (ql *QuickList) removeCount(val []byte, count int) int {
	if ql.Len() == 0 {
		return 0
	}

	iter := ql.Find(0)
	delCount := 0
	for !iter.atEnd() {
		v := iter.get().([]byte)
		if bytes.Equal(v, val) {
			iter.remove()
			delCount++
			if delCount == count {
				return delCount
			}
		} else {
			iter.next()
		}
	}
	return delCount
}

func (ql *QuickList) removeCountReverse(val []byte, count int) int {
	if ql.Len() == 0 {
		return 0
	}

	iter := ql.Find(ql.Len() - 1)
	delCount := 0
	for !iter.atBegin() {
		v := iter.get().([]byte)
		if bytes.Equal(v, val) {
			iter.remove()
			delCount++
			if delCount == count {
				return delCount
			}
		}
		iter.prev()
	}

	return delCount
}

// 若下标值超出范围，则设为边界值
// 若start >= 列表长度，返回空列表
func (ql *QuickList) Range(start, stop int) [][]byte {
	if ql == nil || ql.Len() == 0 {
		return nil
	}

	if start < 0 {
		start += ql.len
		if start < 0 {
			start = 0
		}
	} else if start >= ql.len {
		return nil
	}
	if stop < 0 {
		stop += ql.len
		if stop < 0 {
			return nil
		}
	} else if stop >= ql.len {
		stop = ql.len - 1
	}

	if start > stop {
		return nil
	}

	vals := make([][]byte, 0, stop-start+1)
	iter := ql.Find(start)
	for i := start; i <= stop; i++ {
		vals = append(vals, iter.get().([]byte))
		iter.next()
	}
	return vals
}

// 支持负数下标
func (ql *QuickList) RemoveByIndex(index int) []byte {
	if index < 0 {
		index += ql.Len()
		if index < 0 {
			return nil
		}
	} else if index >= ql.len {
		return nil
	}

	iter := ql.Find(index)
	if iter != nil {
		v := iter.remove()
		return v.([]byte)
	}
	return nil
}

func (ql *QuickList) GetByIndex(index int) []byte {
	if index < 0 {
		index += ql.Len()
		if index < 0 {
			return nil
		}
	} else if index >= ql.len {
		return nil
	}

	iter := ql.Find(index)
	if iter != nil {
		v := iter.get()
		return v.([]byte)
	}
	return nil
}

func (ql *QuickList) Set(index int, val any) bool {
	if index < 0 {
		index += ql.Len()
		if index < 0 {
			return false
		}
	} else if index >= ql.len {
		return false
	}

	iter := ql.Find(index)
	page := iter.page()
	page[iter.offset] = val
	iter.ele.Value = page
	return true
}
