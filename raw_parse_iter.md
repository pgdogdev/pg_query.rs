             Root
          /   |   \
      left   mid   right

1. Stack: Node(Root), result: []
2. Stack: Collect(Root), Node(left), Node(mid), Node(right), result: []
3. Stack: Collect(Root), Node(left), Node(mid), Collect(right), Node(right_1), Node(right_2), result: []
4. Stack: Collect(Root), Node(left), Node(mid), Collect(right), Node(right_1), result: [right_2]
5. Stack: Collect(Root), Node(left), Node(mid), Collect(right), Collect(right_1), Node(right_1_1), Node(right_1_2), result: [right_2]
6. Stack: Collect(Root), Node(left), Node(mid), Collect(right), Collect(right_1), Node(right_1_1), result: [right_2, right_1_2]
7. Stack: Collect(Root), Node(left), Node(mid), Collect(right), Collect(right_1), result: [right_2, right_1_2, right_1_1]
8. Stack: Collect(Root), Node(left), Node(mid), Collect(right), result: [right_2, right_1]
9. Stack: Collect(Root), Node(left), Node(mid), result: [right]
10. Stack: Collect(Root), Node(left), result: [right, mid]
11. Stack: Collect(Root), Collect(left), Node(left_1), Node(left_2), result: [right, mid]
