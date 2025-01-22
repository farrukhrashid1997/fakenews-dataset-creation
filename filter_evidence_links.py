import json

def filter_results(search_results):
    """Filtering the results to remove social media and fact-checking links,
    as well as results with specific terms in the snippet or title."""
    excluded_domains = [
        'instagram.com',
        'linkedin.com',
        "fact-check",
        "snopes",
        "youtube.com",
        "youtube", 
        "tiktok",
        "politifact.com",
        "factcheck.org",
    ]
    
    excluded_terms = ["snopes"]

    for _, evidence_links in search_results.items():
        evidence_links["evidence"] = [
            evidence_link
            for evidence_link in evidence_links["evidence"]
            if not (
                any(domain in evidence_link['link'].lower() for domain in excluded_domains) or
                any(term in evidence_link.get("snippet", "").lower() for term in excluded_terms) or
                any(term in evidence_link.get("title", "").lower() for term in excluded_terms)
            )
        ]

    return search_results

if __name__ == "__main__":
    with open('output/search_results_latest.json', 'r') as f:
        search_results = json.load(f)
    
    filtered_results = filter_results(search_results)

    with open('output/search_results_latest.json', 'w') as f:
        json.dump(filtered_results, f, indent=4, ensure_ascii=False)
