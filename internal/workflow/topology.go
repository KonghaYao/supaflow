package workflow

import "sort"

func loopAwareTopologicalOrder(nodes map[string]NodeDefinition, outgoing, incoming map[string][]string) ([]string, bool) {
	normalOutgoing := cloneAdjacency(outgoing)
	normalIncoming := cloneAdjacency(incoming)
	for source, targets := range outgoing {
		for _, target := range targets {
			if nodes[target].Type != NodeTypeLoop || !walk(target, outgoing)[source] {
				continue
			}
			normalOutgoing[source] = removeNode(normalOutgoing[source], target)
			normalIncoming[target] = removeNode(normalIncoming[target], source)
		}
	}
	return topologicalOrder(nodes, normalOutgoing, normalIncoming)
}

func cloneAdjacency(source map[string][]string) map[string][]string {
	cloned := make(map[string][]string, len(source))
	for node, values := range source {
		cloned[node] = append([]string(nil), values...)
	}
	return cloned
}

func removeNode(values []string, target string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != target {
			result = append(result, value)
		}
	}
	return result
}

func topologicalOrder(nodes map[string]NodeDefinition, outgoing, incoming map[string][]string) ([]string, bool) {
	degrees := make(map[string]int, len(nodes))
	ready := make([]string, 0)
	for id := range nodes {
		degrees[id] = len(incoming[id])
		if degrees[id] == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	order := make([]string, 0, len(nodes))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		children := append([]string(nil), outgoing[id]...)
		sort.Strings(children)
		for _, child := range children {
			degrees[child]--
			if degrees[child] == 0 {
				ready = append(ready, child)
				sort.Strings(ready)
			}
		}
	}
	return order, len(order) != len(nodes)
}

func walk(start string, adjacency map[string][]string) map[string]bool {
	visited := make(map[string]bool)
	stack := []string{start}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[id] {
			continue
		}
		visited[id] = true
		stack = append(stack, adjacency[id]...)
	}
	return visited
}
