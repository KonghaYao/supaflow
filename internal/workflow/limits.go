package workflow

import "fmt"

const (
	// MaxDefinitionNodes is the hard compile-time node bound for one workflow.
	MaxDefinitionNodes = 512
	// MaxDefinitionEdges is the hard compile-time edge bound for one workflow.
	MaxDefinitionEdges = 2048
	// MaxNodeOutDegree bounds durable attempts emitted by one completed node.
	MaxNodeOutDegree = 64
	// MaxSuccessorPayloadBytes bounds the aggregate serialized successor batch.
	MaxSuccessorPayloadBytes int64 = 16 << 20
)

func (s *validationState) validateCollectionLimits() bool {
	exceeded := false
	if len(s.definition.Nodes) > MaxDefinitionNodes {
		s.add("node_limit_exceeded", "nodes", fmt.Sprintf("workflow may contain at most %d nodes", MaxDefinitionNodes))
		exceeded = true
	}
	if len(s.definition.Edges) > MaxDefinitionEdges {
		s.add("edge_limit_exceeded", "edges", fmt.Sprintf("workflow may contain at most %d edges", MaxDefinitionEdges))
		exceeded = true
	}
	return exceeded
}

func (s *validationState) validateFanoutLimits() {
	portFanout := make(map[string]map[string]int)
	for _, edge := range s.definition.Edges {
		if portFanout[edge.FromNode] == nil {
			portFanout[edge.FromNode] = make(map[string]int)
		}
		portFanout[edge.FromNode][edge.FromPort]++
	}
	for nodeID, outgoing := range s.outgoing {
		if len(outgoing) > MaxNodeOutDegree {
			s.add("fanout_limit_exceeded", "nodes."+nodeID, fmt.Sprintf("node may have at most %d outgoing edges", MaxNodeOutDegree))
		}
		spec, ok := s.specs[nodeID]
		if !ok || spec.MaximumOutputBytes <= 0 {
			continue
		}
		for _, count := range portFanout[nodeID] {
			if int64(count) > MaxSuccessorPayloadBytes/spec.MaximumOutputBytes {
				s.add("payload_amplification_limit_exceeded", "nodes."+nodeID,
					fmt.Sprintf("one output port may amplify to at most %d serialized successor bytes", MaxSuccessorPayloadBytes))
				break
			}
		}
	}
}
