# Unified Scorer (combines semantic, structural, contextual results)

class UnifiedScorer:
    def __init__(self):
        pass

    def score(self, semantic_score, structural_score, contextual_score):
        return (semantic_score * 0.5) + (structural_score * 0.3) + (contextual_score * 0.2)

    def rank_results(self, candidates):
        return sorted(candidates, key=lambda x: x['score'], reverse=True)

# Example
if __name__ == "__main__":
    scorer = UnifiedScorer()
    candidates = [
        {"result": "code1", "score": scorer.score(0.8, 0.6, 0.5)},
        {"result": "code2", "score": scorer.score(0.6, 0.7, 0.4)},
    ]
    ranked = scorer.rank_results(candidates)
    for r in ranked:
        print(r)