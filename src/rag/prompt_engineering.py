import json

from search_titles import search_titles


SYSTEM_PROMPT = """
You are an OTT content discovery assistant.

Your job is to recommend relevant movie or TV titles based on the
user's natural-language request.

Rules:
1. Use only titles returned by the retrieval system.
2. Do not invent titles, content IDs, or metadata.
3. Prefer the most semantically relevant results.
4. Return a concise recommendation explanation.
5. Return the final response in the required structured format.
"""


FEW_SHOT_EXAMPLES = [
    {
        "user_query": "action movies",
        "expected_behavior": "Return the most relevant action-oriented titles from retrieval."
    },
    {
        "user_query": "movies for a relaxing evening",
        "expected_behavior": "Return titles whose retrieved descriptions are relevant to a relaxing viewing experience."
    }
]


def build_prompt(user_query, retrieved_results):
    """Build a prompt using system instructions, examples, and retrieved context."""

    context = []

    for rank, result in enumerate(retrieved_results, start=1):
        context.append(
            {
                "rank": rank,
                "title": result["title"],
                "content_id": result["content_id"],
                "document": result["document"],
                "score": round(float(result["score"]), 4),
            }
        )

    return {
        "system_prompt": SYSTEM_PROMPT.strip(),
        "few_shot_examples": FEW_SHOT_EXAMPLES,
        "user_query": user_query,
        "retrieved_context": context,
        "output_schema": {
            "query": "string",
            "recommendations": [
                {
                    "rank": "integer",
                    "title": "string",
                    "content_id": "string",
                    "reason": "string"
                }
            ]
        }
    }


def structured_response(user_query, retrieved_results):
    """Create a deterministic structured response from retrieved results."""

    recommendations = []

    for rank, result in enumerate(retrieved_results, start=1):
        recommendations.append(
            {
                "rank": rank,
                "title": result["title"],
                "content_id": result["content_id"],
                "reason": (
                    f"Retrieved as a relevant match for '{user_query}' "
                    f"with semantic score {float(result['score']):.4f}."
                )
            }
        )

    return {
        "query": user_query,
        "recommendations": recommendations
    }


def run_discovery(query, top_k=5):
    """Run semantic retrieval and build the prompt-engineered response."""

    results = search_titles(
        query,
        top_k=top_k
    )

    prompt = build_prompt(query, results)

    response = structured_response(query, results)

    print("\n===== PROMPT ENGINEERING =====")
    print("\nSystem prompt:")
    print(SYSTEM_PROMPT.strip())

    print("\nFew-shot examples:")
    print(json.dumps(FEW_SHOT_EXAMPLES, indent=2))

    print("\n===== STRUCTURED RESPONSE =====")
    print(json.dumps(response, indent=2))

    return prompt, response


if __name__ == "__main__":
    run_discovery(
        "action movies",
        top_k=5
    )
