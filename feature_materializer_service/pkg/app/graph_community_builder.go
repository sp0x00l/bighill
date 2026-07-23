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

const (
	graphCommunityReportMaxTitleNodes     = 3
	graphCommunityReportMaxEntityLines    = 8
	graphCommunityReportMaxRelationLines  = 8
	graphCommunityReportMaxEvidenceChunks = 5
)

type disabledGraphCommunityReporter struct{}

func NewDisabledGraphCommunityReporter() GraphCommunityReporter {
	log.Trace("NewDisabledGraphCommunityReporter")

	return disabledGraphCommunityReporter{}
}

func (disabledGraphCommunityReporter) BuildGraphCommunities(_ context.Context, materialization *model.GraphMaterialization, _ *model.EmbeddingSnapshot) (*model.GraphMaterialization, error) {
	log.Trace("disabledGraphCommunityReporter BuildGraphCommunities")

	return materialization, nil
}

type embeddingGraphCommunityReporter struct {
	providerFactory QueryEmbeddingProviderFactory
}

func NewEmbeddingGraphCommunityReporter(providerFactory QueryEmbeddingProviderFactory) GraphCommunityReporter {
	log.Trace("NewEmbeddingGraphCommunityReporter")

	if providerFactory == nil {
		return NewDisabledGraphCommunityReporter()
	}
	return &embeddingGraphCommunityReporter{providerFactory: providerFactory}
}

func (r *embeddingGraphCommunityReporter) BuildGraphCommunities(ctx context.Context, materialization *model.GraphMaterialization, embeddingSnapshot *model.EmbeddingSnapshot) (*model.GraphMaterialization, error) {
	log.Trace("embeddingGraphCommunityReporter BuildGraphCommunities")

	if materialization == nil || materialization.Snapshot == nil {
		return nil, domain.ErrGraphCommunityReport.Extend("graph materialization is required")
	}
	if embeddingSnapshot == nil {
		return nil, domain.ErrGraphCommunityReport.Extend("embedding snapshot is required")
	}
	materialization = graphMaterializationWithCommunities(materialization)
	if len(materialization.CommunityReports) == 0 {
		return materialization, nil
	}
	provider, err := r.providerFactory(embeddingStrategyFromSnapshot(embeddingSnapshot))
	if err != nil {
		return nil, fmt.Errorf("%w: create graph community embedding provider: %w", domain.ErrGraphCommunityReport, err)
	}
	if provider.Dimensions() != embeddingSnapshot.EmbeddingDimensions {
		return nil, domain.ErrGraphCommunityReport.Extend("graph community embedding provider dimensions do not match graph embedding snapshot")
	}
	texts := make([]string, len(materialization.CommunityReports))
	for i, report := range materialization.CommunityReports {
		texts[i] = report.EmbeddingText
	}
	vectors, err := provider.Embed(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("%w: embed graph community reports: %w", domain.ErrGraphCommunityReport, err)
	}
	if len(vectors) != len(materialization.CommunityReports) {
		return nil, domain.ErrGraphCommunityReport.Extend("graph community embedding provider returned unexpected vector count")
	}
	resolved := *materialization
	resolved.CommunityReports = append([]model.GraphCommunityReport(nil), materialization.CommunityReports...)
	for i, vector := range vectors {
		if len(vector) != embeddingSnapshot.EmbeddingDimensions {
			return nil, domain.ErrGraphCommunityReport.Extend("graph community embedding vector dimensions do not match graph embedding snapshot")
		}
		resolved.CommunityReports[i].Vector = normalizeVector(vector)
	}
	return &resolved, nil
}

func graphMaterializationWithCommunities(materialization *model.GraphMaterialization) *model.GraphMaterialization {
	log.Trace("graphMaterializationWithCommunities")

	if materialization == nil || materialization.Snapshot == nil || len(materialization.Nodes) == 0 {
		return materialization
	}
	nodes := append([]model.GraphNode(nil), materialization.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].EntityKey < nodes[j].EntityKey })
	nodeIndexByKey := make(map[string]int, len(nodes))
	for i, node := range nodes {
		nodeIndexByKey[node.EntityKey] = i
	}

	components := newGraphEntityDisjointSet(len(nodes))
	for _, edge := range materialization.Edges {
		sourceIndex, sourceOK := nodeIndexByKey[edge.SourceEntityKey]
		targetIndex, targetOK := nodeIndexByKey[edge.TargetEntityKey]
		if !sourceOK || !targetOK {
			continue
		}
		components.union(sourceIndex, targetIndex)
	}

	nodeIndexesByRoot := map[int][]int{}
	for i := range nodes {
		root := components.find(i)
		nodeIndexesByRoot[root] = append(nodeIndexesByRoot[root], i)
	}
	groups := graphCommunityGroups(nodeIndexesByRoot, nodes)
	communityKeyByEntityKey := make(map[string]string, len(nodes))
	communityEdgeCountByKey := map[string]int{}
	edgesByCommunityKey := map[string][]model.GraphEdge{}

	for i, group := range groups {
		communityKey := graphCommunityKey(i, group)
		for _, node := range group {
			communityKeyByEntityKey[node.EntityKey] = communityKey
		}
	}
	for _, edge := range materialization.Edges {
		sourceCommunity := communityKeyByEntityKey[edge.SourceEntityKey]
		targetCommunity := communityKeyByEntityKey[edge.TargetEntityKey]
		if sourceCommunity == "" || sourceCommunity != targetCommunity {
			continue
		}
		communityEdgeCountByKey[sourceCommunity]++
		edgesByCommunityKey[sourceCommunity] = append(edgesByCommunityKey[sourceCommunity], edge)
	}

	chunksByEntityKey := map[string][]model.GraphNodeChunk{}
	for _, chunk := range materialization.NodeChunks {
		chunksByEntityKey[chunk.EntityKey] = append(chunksByEntityKey[chunk.EntityKey], chunk)
	}

	communities := make([]model.GraphCommunity, 0, len(groups))
	members := make([]model.GraphCommunityMember, 0, len(nodes))
	reports := make([]model.GraphCommunityReport, 0, len(groups))
	for i, group := range groups {
		communityKey := graphCommunityKey(i, group)
		edges := edgesByCommunityKey[communityKey]
		title := graphCommunityTitle(group)
		summary := graphCommunitySummary(group, edges)
		rank := graphCommunityRank(group, edges)
		community := model.GraphCommunity{
			GraphSnapshotID: materialization.Snapshot.GraphSnapshotID,
			DatasetID:       materialization.Snapshot.DatasetID,
			UserID:          materialization.Snapshot.UserID,
			OrgID:           materialization.Snapshot.OrgID,
			CommunityKey:    communityKey,
			Algorithm:       model.GraphCommunityAlgorithmConnectedComponents,
			Level:           0,
			Title:           title,
			Summary:         summary,
			Rank:            rank,
			EntityCount:     len(group),
			EdgeCount:       communityEdgeCountByKey[communityKey],
		}
		communities = append(communities, community)
		for _, node := range group {
			members = append(members, model.GraphCommunityMember{
				GraphSnapshotID: materialization.Snapshot.GraphSnapshotID,
				EntityKey:       node.EntityKey,
				CommunityKey:    communityKey,
				DatasetID:       materialization.Snapshot.DatasetID,
				UserID:          materialization.Snapshot.UserID,
				OrgID:           materialization.Snapshot.OrgID,
			})
		}
		reportText := graphCommunityReportText(community, group, edges, chunksByEntityKey)
		reports = append(reports, model.GraphCommunityReport{
			GraphSnapshotID:     materialization.Snapshot.GraphSnapshotID,
			EmbeddingSnapshotID: materialization.Snapshot.EmbeddingSnapshotID,
			DatasetID:           materialization.Snapshot.DatasetID,
			UserID:              materialization.Snapshot.UserID,
			OrgID:               materialization.Snapshot.OrgID,
			CommunityKey:        communityKey,
			Level:               community.Level,
			Title:               title,
			Summary:             summary,
			ReportText:          reportText,
			Rank:                rank,
			ReportVersion:       model.GraphCommunityReportExtractiveV1,
			EmbeddingText:       reportText,
		})
	}

	resolved := *materialization
	resolved.Communities = communities
	resolved.CommunityMembers = members
	resolved.CommunityReports = reports
	return &resolved
}

func graphCommunityGroups(nodeIndexesByRoot map[int][]int, nodes []model.GraphNode) [][]model.GraphNode {
	log.Trace("graphCommunityGroups")

	groups := make([][]model.GraphNode, 0, len(nodeIndexesByRoot))
	for _, indexes := range nodeIndexesByRoot {
		group := make([]model.GraphNode, 0, len(indexes))
		for _, index := range indexes {
			group = append(group, nodes[index])
		}
		sort.Slice(group, func(i, j int) bool { return graphCommunityNodeLess(group[i], group[j]) })
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i][0].EntityKey < groups[j][0].EntityKey
	})
	return groups
}

func graphCommunityKey(index int, nodes []model.GraphNode) string {
	log.Trace("graphCommunityKey")

	if len(nodes) == 0 {
		return fmt.Sprintf("community:%03d", index+1)
	}
	return fmt.Sprintf("community:%03d:%s", index+1, nodes[0].EntityKey)
}

func graphCommunityNodeLess(left model.GraphNode, right model.GraphNode) bool {
	log.Trace("graphCommunityNodeLess")

	if left.MentionCount != right.MentionCount {
		return left.MentionCount > right.MentionCount
	}
	return left.EntityKey < right.EntityKey
}

func graphCommunityTitle(nodes []model.GraphNode) string {
	log.Trace("graphCommunityTitle")

	names := make([]string, 0, graphCommunityReportMaxTitleNodes)
	for _, node := range nodes {
		name := strings.TrimSpace(node.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
		if len(names) == graphCommunityReportMaxTitleNodes {
			break
		}
	}
	if len(names) == 0 {
		return "Untitled graph community"
	}
	return strings.Join(names, " / ")
}

func graphCommunitySummary(nodes []model.GraphNode, edges []model.GraphEdge) string {
	log.Trace("graphCommunitySummary")

	if len(nodes) == 1 {
		return fmt.Sprintf("%s is an isolated graph community with 1 entity.", graphCommunityTitle(nodes))
	}
	return fmt.Sprintf("%s is a graph community with %d entities and %d internal relationships.", graphCommunityTitle(nodes), len(nodes), len(edges))
}

func graphCommunityRank(nodes []model.GraphNode, edges []model.GraphEdge) float64 {
	log.Trace("graphCommunityRank")

	var rank float64
	for _, node := range nodes {
		rank += float64(node.MentionCount)
	}
	for _, edge := range edges {
		rank += edge.Weight
	}
	if rank <= 0 {
		return float64(len(nodes))
	}
	return rank
}

func graphCommunityReportText(community model.GraphCommunity, nodes []model.GraphNode, edges []model.GraphEdge, chunksByEntityKey map[string][]model.GraphNodeChunk) string {
	log.Trace("graphCommunityReportText")

	parts := []string{
		"Title: " + community.Title,
		"Summary: " + community.Summary,
	}
	entityLines := graphCommunityEntityLines(nodes, graphCommunityReportMaxEntityLines)
	if len(entityLines) > 0 {
		parts = append(parts, "Entities:\n- "+strings.Join(entityLines, "\n- "))
	}
	relationLines := graphCommunityRelationLines(edges, graphCommunityReportMaxRelationLines)
	if len(relationLines) > 0 {
		parts = append(parts, "Relationships:\n- "+strings.Join(relationLines, "\n- "))
	}
	evidenceLines := graphCommunityEvidenceLines(nodes, chunksByEntityKey, graphCommunityReportMaxEvidenceChunks)
	if len(evidenceLines) > 0 {
		parts = append(parts, "Evidence:\n- "+strings.Join(evidenceLines, "\n- "))
	}
	return strings.Join(parts, "\n\n")
}

func graphCommunityEntityLines(nodes []model.GraphNode, limit int) []string {
	log.Trace("graphCommunityEntityLines")

	lines := make([]string, 0, min(limit, len(nodes)))
	for _, node := range nodes {
		name := strings.TrimSpace(node.Name)
		if name == "" {
			name = node.EntityKey
		}
		line := name
		if entityType := strings.TrimSpace(node.Type); entityType != "" {
			line += " (" + entityType + ")"
		}
		if description := strings.TrimSpace(node.Description); description != "" {
			line += ": " + description
		}
		lines = append(lines, line)
		if len(lines) == limit {
			break
		}
	}
	return lines
}

func graphCommunityRelationLines(edges []model.GraphEdge, limit int) []string {
	log.Trace("graphCommunityRelationLines")

	sortedEdges := append([]model.GraphEdge(nil), edges...)
	sort.Slice(sortedEdges, func(i, j int) bool {
		if sortedEdges[i].Weight != sortedEdges[j].Weight {
			return sortedEdges[i].Weight > sortedEdges[j].Weight
		}
		if sortedEdges[i].SourceEntityKey == sortedEdges[j].SourceEntityKey {
			return sortedEdges[i].TargetEntityKey < sortedEdges[j].TargetEntityKey
		}
		return sortedEdges[i].SourceEntityKey < sortedEdges[j].SourceEntityKey
	})
	lines := make([]string, 0, min(limit, len(sortedEdges)))
	for _, edge := range sortedEdges {
		relationType := strings.TrimSpace(edge.RelationType)
		if relationType == "" {
			relationType = "RELATED_TO"
		}
		line := edge.SourceEntityKey + " " + relationType + " " + edge.TargetEntityKey
		if description := strings.TrimSpace(edge.Description); description != "" {
			line += ": " + description
		}
		lines = append(lines, line)
		if len(lines) == limit {
			break
		}
	}
	return lines
}

func graphCommunityEvidenceLines(nodes []model.GraphNode, chunksByEntityKey map[string][]model.GraphNodeChunk, limit int) []string {
	log.Trace("graphCommunityEvidenceLines")

	lines := make([]string, 0, limit)
	seen := map[string]struct{}{}
	for _, node := range nodes {
		chunks := append([]model.GraphNodeChunk(nil), chunksByEntityKey[node.EntityKey]...)
		sort.Slice(chunks, func(i, j int) bool { return chunks[i].ChunkIndex < chunks[j].ChunkIndex })
		for _, chunk := range chunks {
			text := strings.Join(strings.Fields(strings.TrimSpace(chunk.SourceText)), " ")
			if text == "" {
				continue
			}
			if _, ok := seen[text]; ok {
				continue
			}
			seen[text] = struct{}{}
			lines = append(lines, text)
			if len(lines) == limit {
				return lines
			}
		}
	}
	return lines
}
