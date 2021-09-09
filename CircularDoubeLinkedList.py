class CacheObject():
    def __init__(self, id, size):
        self.id = id
        self.size = size

class Node():
    def __init__(self, object=None, prev=None, next=None):
        self.object = object
        self.prev = prev
        self.next = next

# 双端链表
class CircularDoubeLinkedList():
    def __init__(self):
        node = Node()
        node.prev, node.next = node, node
        self.root_node = node

    # 头节点
    def head_node(self):
        return self.root_node.next

    # 尾节点
    def tail_node(self):
        return self.root_node.prev

    # 移除一个节点
    def remove(self, node):
        if node is self.root_node:
            return
        node.prev.next = node.next
        node.next.prev = node.prev

    # 追加一个节点到链表的尾部
    def append(self, node):
        tail_node = self.tail_node()
        tail_node.next = node
        node.next = self.root_node
        node.prev = tail_node
        self.root_node.prev = node
