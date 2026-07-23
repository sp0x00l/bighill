package app

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

const defaultGraphEntityResolutionThreshold = 0.92

type noopGraphEntityResolver struct{}

func NewNoopGraphEntityResolver() GraphEntityResolver {
	log.Trace("NewNoopGraphEntityResolver")

	return noopGraphEntityResolver{}
}

func (noopGraphEntityResolver) ResolveGraphEntities(_ context.Context, materialization *model.GraphMaterialization, _ *model.EmbeddingSnapshot) (*model.GraphMaterialization, error) {
	log.Trace("noopGraphEntityResolver ResolveGraphEntities")

	return materialization, nil
}

type embeddingGraphEntityResolver struct {
	providerFactory     QueryEmbeddingProviderFactory
	similarityThreshold float64
}

func NewEmbeddingGraphEntityResolver(providerFactory QueryEmbeddingProviderFactory, similarityThreshold float64) GraphEntityResolver {
	log.Trace("NewEmbeddingGraphEntityResolver")

	if providerFactory == nil {
		return NewNoopGraphEntityResolver()
	}
	if similarityThreshold <= 0 || similarityThreshold > 1 {
		similarityThreshold = defaultGraphEntityResolutionThreshold
	}
	return &embeddingGraphEntityResolver{
		providerFactory:     providerFactory,
		similarityThreshold: similarityThreshold,
	}
}

func (r *embeddingGraphEntityResolver) ResolveGraphEntities(ctx context.Context, materialization *model.GraphMaterialization, embeddingSnapshot *model.EmbeddingSnapshot) (*model.GraphMaterialization, error) {
	log.Trace("embeddingGraphEntityResolver ResolveGraphEntities")

	if materialization == nil || materialization.Snapshot == nil {
		return nil, domain.ErrGraphEntityResolution.Extend("graph materialization is required")
	}
	if len(materialization.Nodes) == 0 {
		resolved := *materialization
		resolved.NodeAliases = nil
		resolved.NodeEmbeddings = nil
		return &resolved, nil
	}
	if embeddingSnapshot == nil {
		return nil, domain.ErrGraphEntityResolution.Extend("embedding snapshot is required")
	}
	provider, err := r.providerFactory(embeddingStrategyFromSnapshot(embeddingSnapshot))
	if err != nil {
		return nil, fmt.Errorf("%w: create graph entity embedding provider: %w", domain.ErrGraphEntityResolution, err)
	}
	if provider.Dimensions() != embeddingSnapshot.EmbeddingDimensions {
		return nil, domain.ErrGraphEntityResolution.Extend("graph entity embedding provider dimensions do not match graph embedding snapshot")
	}

	nodes := append([]model.GraphNode(nil), materialization.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].EntityKey < nodes[j].EntityKey })
	texts := make([]string, len(nodes))
	for i, node := range nodes {
		texts[i] = graphNodeEmbeddingText(node)
	}
	vectors, err := provider.Embed(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("%w: embed graph entities: %w", domain.ErrGraphEntityResolution, err)
	}
	if len(vectors) != len(nodes) {
		return nil, domain.ErrGraphEntityResolution.Extend("graph entity embedding provider returned unexpected vector count")
	}
	for i, vector := range vectors {
		if len(vector) != embeddingSnapshot.EmbeddingDimensions {
			return nil, domain.ErrGraphEntityResolution.Extend("graph entity embedding vector dimensions do not match graph embedding snapshot")
		}
		vectors[i] = normalizeVector(vector)
	}

	canonicalByKey := graphCanonicalEntityKeys(nodes, vectors, r.similarityThreshold)
	return graphResolveMaterialization(materialization, embeddingSnapshot, nodes, vectors, canonicalByKey), nil
}

func graphCanonicalEntityKeys(nodes []model.GraphNode, vectors [][]float32, threshold float64) map[string]string {
	log.Trace("graphCanonicalEntityKeys")

	clusters := newGraphEntityDisjointSet(len(nodes))
	// This exact pairwise pass is snapshot-bounded; indexed or blocked candidate generation is the next scale step.
	for i, node := range nodes {
		if node.EntityKey == "" {
			continue
		}
		nodeType := graphNormalizedType(node.Type)
		for j := i + 1; j < len(nodes); j++ {
			candidate := nodes[j]
			if candidate.EntityKey == "" {
				continue
			}
			if graphNormalizedType(candidate.Type) != nodeType {
				continue
			}
			if graphCosineSimilarity(vectors[i], vectors[j]) >= threshold {
				clusters.union(i, j)
			}
		}
	}

	canonicalIndexByRoot := make(map[int]int, len(nodes))
	for i, node := range nodes {
		if node.EntityKey == "" {
			continue
		}
		root := clusters.find(i)
		canonicalIndex, ok := canonicalIndexByRoot[root]
		if !ok || graphPreferCanonicalNode(node, nodes[canonicalIndex]) {
			canonicalIndexByRoot[root] = i
		}
	}

	canonicalByKey := make(map[string]string, len(nodes))
	for i, node := range nodes {
		if node.EntityKey == "" {
			continue
		}
		canonicalIndex, ok := canonicalIndexByRoot[clusters.find(i)]
		if !ok {
			canonicalByKey[node.EntityKey] = node.EntityKey
			continue
		}
		canonicalByKey[node.EntityKey] = nodes[canonicalIndex].EntityKey
	}
	return canonicalByKey
}

type graphEntityDisjointSet struct {
	parent []int
	rank   []int
}

func newGraphEntityDisjointSet(size int) graphEntityDisjointSet {
	log.Trace("newGraphEntityDisjointSet")

	parent := make([]int, size)
	for i := range parent {
		parent[i] = i
	}
	return graphEntityDisjointSet{
		parent: parent,
		rank:   make([]int, size),
	}
}

func (s *graphEntityDisjointSet) find(index int) int {
	log.Trace("graphEntityDisjointSet find")

	if s.parent[index] != index {
		s.parent[index] = s.find(s.parent[index])
	}
	return s.parent[index]
}

func (s *graphEntityDisjointSet) union(left int, right int) {
	log.Trace("graphEntityDisjointSet union")

	leftRoot := s.find(left)
	rightRoot := s.find(right)
	if leftRoot == rightRoot {
		return
	}
	if s.rank[leftRoot] < s.rank[rightRoot] {
		s.parent[leftRoot] = rightRoot
		return
	}
	if s.rank[leftRoot] > s.rank[rightRoot] {
		s.parent[rightRoot] = leftRoot
		return
	}
	s.parent[rightRoot] = leftRoot
	s.rank[leftRoot]++
}

func graphPreferCanonicalNode(candidate model.GraphNode, current model.GraphNode) bool {
	log.Trace("graphPreferCanonicalNode")

	if candidate.MentionCount != current.MentionCount {
		return candidate.MentionCount > current.MentionCount
	}
	candidateName := strings.TrimSpace(candidate.Name)
	currentName := strings.TrimSpace(current.Name)
	if candidateName != "" && currentName == "" {
		return true
	}
	if candidateName == "" && currentName != "" {
		return false
	}
	return candidate.EntityKey < current.EntityKey
}

func graphResolveMaterialization(materialization *model.GraphMaterialization, embeddingSnapshot *model.EmbeddingSnapshot, nodes []model.GraphNode, vectors [][]float32, canonicalByKey map[string]string) *model.GraphMaterialization {
	log.Trace("graphResolveMaterialization")

	nodeByKey := make(map[string]model.GraphNode, len(nodes))
	vectorByKey := make(map[string][]float32, len(nodes))
	for i, node := range nodes {
		nodeByKey[node.EntityKey] = node
		vectorByKey[node.EntityKey] = vectors[i]
	}

	canonicalNodes := map[string]model.GraphNode{}
	clusterVectors := map[string][][]float32{}
	aliases := make([]model.GraphNodeAlias, 0, len(nodes))
	for _, node := range nodes {
		canonicalKey := canonicalByKey[node.EntityKey]
		if canonicalKey == "" {
			canonicalKey = node.EntityKey
		}
		canonical := canonicalNodes[canonicalKey]
		if canonical.EntityKey == "" {
			canonical = nodeByKey[canonicalKey]
			canonical.MentionCount = 0
		}
		canonical.MentionCount += node.MentionCount
		if canonical.Description == "" && strings.TrimSpace(node.Description) != "" {
			canonical.Description = strings.TrimSpace(node.Description)
		}
		canonicalNodes[canonicalKey] = canonical
		clusterVectors[canonicalKey] = append(clusterVectors[canonicalKey], vectorByKey[node.EntityKey])
		aliases = append(aliases, model.GraphNodeAlias{
			GraphSnapshotID:    materialization.Snapshot.GraphSnapshotID,
			CanonicalEntityKey: canonicalKey,
			SourceEntityKey:    node.EntityKey,
			Alias:              node.Name,
			Type:               node.Type,
			DatasetID:          materialization.Snapshot.DatasetID,
			UserID:             materialization.Snapshot.UserID,
			OrgID:              materialization.Snapshot.OrgID,
		})
	}

	outNodes := make([]model.GraphNode, 0, len(canonicalNodes))
	for _, node := range canonicalNodes {
		outNodes = append(outNodes, node)
	}
	sort.Slice(outNodes, func(i, j int) bool { return outNodes[i].EntityKey < outNodes[j].EntityKey })
	sort.Slice(aliases, func(i, j int) bool {
		if aliases[i].CanonicalEntityKey == aliases[j].CanonicalEntityKey {
			return aliases[i].SourceEntityKey < aliases[j].SourceEntityKey
		}
		return aliases[i].CanonicalEntityKey < aliases[j].CanonicalEntityKey
	})

	resolved := *materialization
	resolved.Nodes = outNodes
	resolved.NodeAliases = aliases
	resolved.NodeEmbeddings = graphResolvedNodeEmbeddings(materialization.Snapshot, embeddingSnapshot, outNodes, aliases, clusterVectors)
	resolved.NodeChunks = graphResolvedNodeChunks(materialization.NodeChunks, canonicalByKey)
	resolved.Edges = graphResolvedEdges(materialization.Edges, canonicalByKey)
	resolved.Snapshot.EntityCount = int64(len(resolved.Nodes))
	resolved.Snapshot.EdgeCount = int64(len(resolved.Edges))
	return &resolved
}

func graphResolvedNodeEmbeddings(snapshot *model.GraphSnapshot, embeddingSnapshot *model.EmbeddingSnapshot, nodes []model.GraphNode, aliases []model.GraphNodeAlias, clusterVectors map[string][][]float32) []model.GraphNodeEmbedding {
	log.Trace("graphResolvedNodeEmbeddings")

	aliasesByCanonical := map[string][]model.GraphNodeAlias{}
	for _, alias := range aliases {
		aliasesByCanonical[alias.CanonicalEntityKey] = append(aliasesByCanonical[alias.CanonicalEntityKey], alias)
	}
	embeddings := make([]model.GraphNodeEmbedding, 0, len(nodes))
	for _, node := range nodes {
		embeddings = append(embeddings, model.GraphNodeEmbedding{
			GraphSnapshotID:     snapshot.GraphSnapshotID,
			EntityKey:           node.EntityKey,
			EmbeddingSnapshotID: embeddingSnapshot.EmbeddingSnapshotID,
			DatasetID:           snapshot.DatasetID,
			UserID:              snapshot.UserID,
			OrgID:               snapshot.OrgID,
			EmbeddingText:       graphCanonicalEmbeddingText(node, aliasesByCanonical[node.EntityKey]),
			Vector:              graphAverageVectors(clusterVectors[node.EntityKey]),
		})
	}
	return embeddings
}

func graphResolvedNodeChunks(chunks []model.GraphNodeChunk, canonicalByKey map[string]string) []model.GraphNodeChunk {
	log.Trace("graphResolvedNodeChunks")

	seen := map[string]struct{}{}
	resolved := make([]model.GraphNodeChunk, 0, len(chunks))
	for _, chunk := range chunks {
		canonicalKey := graphCanonicalKey(canonicalByKey, chunk.EntityKey)
		if canonicalKey == "" {
			continue
		}
		chunk.EntityKey = canonicalKey
		key := canonicalKey + "\x00" + chunk.EmbeddingRecordID.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		resolved = append(resolved, chunk)
	}
	sort.Slice(resolved, func(i, j int) bool {
		if resolved[i].EntityKey == resolved[j].EntityKey {
			return resolved[i].ChunkIndex < resolved[j].ChunkIndex
		}
		return resolved[i].EntityKey < resolved[j].EntityKey
	})
	return resolved
}

func graphResolvedEdges(edges []model.GraphEdge, canonicalByKey map[string]string) []model.GraphEdge {
	log.Trace("graphResolvedEdges")

	edgeByKey := map[string]model.GraphEdge{}
	for _, edge := range edges {
		sourceKey := graphCanonicalKey(canonicalByKey, edge.SourceEntityKey)
		targetKey := graphCanonicalKey(canonicalByKey, edge.TargetEntityKey)
		if sourceKey == "" || targetKey == "" || sourceKey == targetKey {
			continue
		}
		edge.SourceEntityKey = sourceKey
		edge.TargetEntityKey = targetKey
		relationType := strings.TrimSpace(edge.RelationType)
		if relationType == "" {
			relationType = "RELATED_TO"
		}
		key := sourceKey + "\x00" + targetKey + "\x00" + relationType
		existing, ok := edgeByKey[key]
		if !ok || edge.Weight > existing.Weight {
			edge.RelationType = relationType
			edgeByKey[key] = edge
			continue
		}
		if existing.Description == "" && strings.TrimSpace(edge.Description) != "" {
			existing.Description = strings.TrimSpace(edge.Description)
			edgeByKey[key] = existing
		}
	}
	resolved := make([]model.GraphEdge, 0, len(edgeByKey))
	for _, edge := range edgeByKey {
		resolved = append(resolved, edge)
	}
	sort.Slice(resolved, func(i, j int) bool {
		if resolved[i].SourceEntityKey == resolved[j].SourceEntityKey {
			if resolved[i].TargetEntityKey == resolved[j].TargetEntityKey {
				return resolved[i].RelationType < resolved[j].RelationType
			}
			return resolved[i].TargetEntityKey < resolved[j].TargetEntityKey
		}
		return resolved[i].SourceEntityKey < resolved[j].SourceEntityKey
	})
	return resolved
}

func graphCanonicalKey(canonicalByKey map[string]string, entityKey string) string {
	if canonicalKey := canonicalByKey[entityKey]; canonicalKey != "" {
		return canonicalKey
	}
	return entityKey
}

func graphNodeEmbeddingText(node model.GraphNode) string {
	log.Trace("graphNodeEmbeddingText")

	parts := []string{}
	if text := strings.TrimSpace(node.Name); text != "" {
		parts = append(parts, "name: "+text)
	}
	if text := strings.TrimSpace(node.Type); text != "" {
		parts = append(parts, "type: "+text)
	}
	if text := strings.TrimSpace(node.Description); text != "" {
		parts = append(parts, "description: "+text)
	}
	return strings.Join(parts, "\n")
}

func graphCanonicalEmbeddingText(node model.GraphNode, aliases []model.GraphNodeAlias) string {
	log.Trace("graphCanonicalEmbeddingText")

	parts := []string{graphNodeEmbeddingText(node)}
	aliasNames := []string{}
	seen := map[string]struct{}{}
	for _, alias := range aliases {
		normalized := strings.ToLower(strings.TrimSpace(alias.Alias))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		aliasNames = append(aliasNames, strings.TrimSpace(alias.Alias))
	}
	sort.Strings(aliasNames)
	if len(aliasNames) > 0 {
		parts = append(parts, "aliases: "+strings.Join(aliasNames, ", "))
	}
	return strings.Join(parts, "\n")
}

func graphAverageVectors(vectors [][]float32) []float32 {
	log.Trace("graphAverageVectors")

	if len(vectors) == 0 {
		return nil
	}
	out := make([]float32, len(vectors[0]))
	for _, vector := range vectors {
		for i, value := range vector {
			out[i] += value
		}
	}
	scale := float32(len(vectors))
	for i := range out {
		out[i] = out[i] / scale
	}
	return normalizeVector(out)
}

func graphCosineSimilarity(left []float32, right []float32) float64 {
	log.Trace("graphCosineSimilarity")

	if len(left) == 0 || len(left) != len(right) {
		return 0
	}
	var dot float64
	for i, leftValue := range left {
		dot += float64(leftValue) * float64(right[i])
	}
	return dot
}

func graphNormalizedType(entityType string) string {
	log.Trace("graphNormalizedType")

	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(entityType)), " "))
}
