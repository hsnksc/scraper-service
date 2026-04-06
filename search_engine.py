import os
import json
import logging
from openai import OpenAI
from dotenv import load_dotenv
from ddgs import DDGS

load_dotenv()

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY", "ollama"),
    base_url=os.getenv("OPENAI_BASE_URL", "http://localhost:11434/v1"),
)

AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _ai_call(prompt: str, temperature: float = 0.7, stream: bool = False) -> str:
    """AI'yi cagir, response'u don."""
    response = client.chat.completions.create(
        model=AI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=temperature,
        stream=stream,
    )
    if stream:
        full = ""
        for chunk in response:
            if chunk.choices[0].delta.content:
                full += chunk.choices[0].delta.content
        return full
    return response.choices[0].message.content.strip()


def _parse_json(text: str) -> dict | None:
    """Markdown code block temizle, JSON parse et."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    if text.startswith("json"):
        text = text[4:].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


# ---- Interaktif soru üretimi ----

def generate_question(original_query: str, conversation: list[dict]) -> dict:
    """AI, kullanicinin niyetini anlamak icin soru uretir."""
    convo_str = "\n".join(
        f"User: {m['text']}" if m["role"] == "user" else f"AI: {m.get('content', '')}"
        for m in conversation
    )

    prompt = (
        f"The user wants to search for: \"{original_query}\"\n\n"
        f"Conversation so far:\n{convo_str}\n\n"
        f"You need to ask ONE clarifying question to narrow down what the user is looking for. "
        f"Provide answer options so the user can just click instead of typing, but also allow free text.\n\n"
        f"Return ONLY a JSON object:\n"
        f'{{"question": "your question here", "options": ["option 1", "option 2", "option 3", "option 4"]}}\n\n'
        f"Make the question specific and relevant. Options should cover the most important dimensions "
        f"(e.g. skill level, language preference, content type, free vs paid, etc.) based on the query and conversation."
    )

    raw = _ai_call(prompt, temperature=0.7)
    result = _parse_json(raw)
    if result and "question" in result and "options" in result:
        return result
    return {"question": raw, "options": []}


# ---- Hizli on arastirma ----

def quick_search(query: str, answers: dict) -> list[dict]:
    """AI ile daraltilmis 2-3 query uret ve kisa arama yap."""
    convo_summary = "User wants: " + query
    for a in answers.values():
        convo_summary += f"\nPreferrence: {a}"

    prompt = (
        f"Given this user's search intent:\n{convo_summary}\n\n"
        f"Generate exactly 3 Google search queries that will find the most relevant results. "
        f"Return ONLY a JSON array: [\"query1\", \"query2\", \"query3\"]"
    )
    raw = _ai_call(prompt, temperature=0.5)
    queries = _parse_json(raw)
    if not queries or not isinstance(queries, list):
        queries = [query]

    # Kisitli arama (her query'den 5 sonuc)
    return search_google(queries, max_per_query=5)


# ---- Sonraki adim karari ----

def decide_next_step(conversation: list[dict], prelim_results: list[dict]) -> dict:
    """AI karar verir: tekrar soru sor (max 2 kere) veya final aramaya gec."""
    user_answers_count = sum(1 for m in conversation if m["role"] == "user")

    # Max 2 soru sormusuzdan sonra direkt final aramaya gec
    if user_answers_count >= 3 or len(prelim_results) == 0:
        return {"next": "final_search"}

    convo_str = "\n".join(
        f"User: {m['text']}" if m["role"] == "user" else f"AI: {m.get('content', '')}"
        for m in conversation
    )
    results_info = f"Found {len(prelim_results)} preliminary results"

    prompt = (
        f"Based on the conversation, decide if you have enough information to do a final search.\n\n"
        f"Conversation:\n{convo_str}\n"
        f"Preliminary results: {results_info}\n"
        f"User has answered {user_answers_count} question(s).\n\n"
        f"If the user's search needs are clear enough, respond with: "
        f'{{"next": "final_search"}}\n'
        f"If you need one more clarification (max 2 questions total), respond with: "
        f'{{"next": "question"}}\n\n'
        f"Return ONLY the JSON object."
    )

    raw = _ai_call(prompt, temperature=0.3)
    result = _parse_json(raw)
    if result and "next" in result:
        return result

    if "final" in str(raw).lower() or user_answers_count >= 2:
        return {"next": "final_search"}
    return {"next": "question"}


# ---- Google Arama ----

def search_google(queries: list[str], max_per_query: int = 10) -> list[dict]:
    """Her query icin DuckDuckGo aramasi yap, sonuclari birlestir."""
    all_results = []
    seen_urls = set()

    ddgs = DDGS()
    try:
        for query in queries:
            logger.info(f"Searching: {query}")
            try:
                for result in ddgs.text(query, max_results=max_per_query):
                    url = result.get("href", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_results.append({
                            "title": result.get("title") or "",
                            "url": url,
                            "description": result.get("body") or "",
                        })
            except Exception as e:
                logger.warning(f"Search failed for '{query}': {e}")
    finally:
        pass

    return all_results


# ---- Query genisletme (final arama) ----

def expand_query(user_input: str, preferences: dict) -> list[str]:
    """Final arama icin AI ile genisletilmis query'ler."""
    prefs_str = ", ".join(f"{k}: {v}" for k, v in preferences.items())
    prompt = (
        f"Generate 6 highly specific Google search queries for this request.\n\n"
        f"Original: {user_input}\n"
        f"Preferences: {prefs_str}\n\n"
        f"Return ONLY a JSON array of strings: [\"query1\", \"query2\", ...]"
    )
    raw = _ai_call(prompt, temperature=0.7)
    queries = _parse_json(raw)
    return queries if queries and isinstance(queries, list) else [user_input]


# ---- AI Filtreleme ve Ozetleme (streaming) ----

def filter_and_summarize_stream(results: list[dict], original_query: str, preferences: dict):
    """AI ile sonuclari filtrele, sirala, ozetle — streaming ile."""
    if not results:
        yield {"type": "done", "results": []}
        return

    prefs_str = ", ".join(f"{k}: {v}" for k, v in preferences.items())
    results_json = json.dumps(
        [{"title": r["title"], "url": r["url"], "description": r["description"]}
         for r in results[:30]],
        ensure_ascii=False,
        indent=2,
    )

    prompt = (
        f"You are a search quality analyst. The user searched for: '{original_query}'\n"
        f"User preferences: {prefs_str}\n\n"
        f"Given the following search results, analyze each and return:\n"
        f'{{"ranked_results": [\n'
        f'  {{"url": "...", "relevance_score": 0-100, "summary": "1-2 Turkish sentences"}}\n'
        f']}}\n\n'
        f"Rank by relevance descending. Include ALL results.\n"
        f"Return ONLY valid JSON.\n\n"
        f"Search results:\n{results_json}"
    )

    full_text = ""
    for chunk in client.chat.completions.create(
        model=AI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        stream=True,
    ):
        content = chunk.choices[0].delta.content
        if content:
            full_text += content
            yield {"type": "partial", "content": content}

    try:
        if full_text.startswith("```"):
            full_text = full_text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        analysis = json.loads(full_text.replace("json", "").strip())
        ranked = analysis.get("ranked_results", [])
        url_map = {r["url"]: r for r in results}
        enriched = []
        for item in ranked:
            url = item["url"]
            if url in url_map:
                enriched.append({
                    "title": url_map[url]["title"],
                    "url": url,
                    "description": url_map[url]["description"],
                    "cache_url": f"https://webcache.googleusercontent.com/search?q=cache:{url}",
                    "relevance_score": item.get("relevance_score", 50),
                    "summary": item.get("summary", ""),
                })
        yield {"type": "done", "results": enriched}
    except Exception as e:
        yield {"type": "error", "message": str(e)}


# ---- AI Sunum Özeti ----

def generate_summary_presentation(query: str, results: list[dict], preferences: dict) -> dict:
    """AI, tum arama sonuclarindan bir sunum raporu cikarir.
    Emlak aramasi: ortalama m², fiyat, oda sayisi, lokasyon analizi
    Genel arama: karsilastirma, temel bulgular, oneriler"""
    prefs_str = ", ".join(f"{k}: {v}" for k, v in preferences.items())
    results_snippet = json.dumps(
        [{"title": r["title"], "description": r["description"]} for r in results[:20]],
        ensure_ascii=False,
    )

    prompt = (
        f"Based on these search results for: \"{query}\"\n"
        f"Preferences: {prefs_str}\n\n"
        f"Create a comprehensive summary presentation in Turkish. Analyze the search results and provide:\n\n"
        f'{{\n'
        f'  "title": "Search-focused headline",\n'
        f'  "overview": "1-2 paragraph general overview of what was found",\n'
        f'  "key_stats": {{"label": "value"}} pairs (extract any quantifiable data: prices, sizes, ratings, counts, etc.),\n'
        f'  "findings": ["bullet 1", "bullet 2", "bullet 3"],\n'
        f'  "comparisons": ["compare item A vs B", etc.],\n'
        f'  "recommendations": ["suggestion 1", "suggestion 2"]\n'
        f'}}\n\n'
        f"If specific numeric data is not available, use descriptive analysis. "
        f"Extract ALL available numerical data from result titles and descriptions (prices, sizes, bedroom counts, etc.).\n"
        f"Return ONLY valid JSON.\n\n"
        f"Results:\n{results_snippet}"
    )

    raw = _ai_call(prompt, temperature=0.3)
    result = _parse_json(raw)
    if result:
        return result
    return {"title": query, "overview": raw or "Özet oluşturulamadı.", "key_stats": {}, "findings": [], "comparisons": [], "recommendations": []}
