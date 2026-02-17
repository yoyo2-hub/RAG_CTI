# 3_rag_chain.py
"""
Agent CTI avec retrieval intelligent et Phi-3.5
"""

import re
from langchain_community.llms import Ollama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser


def get_llm():
    """Phi-3.5 via Ollama."""
    return Ollama(
        model="phi3.5",
        temperature=0.1,
        num_ctx=4096,
    )


# ══════════════════════════════════════════════
# RETRIEVER INTELLIGENT
# ══════════════════════════════════════════════

def retrieve_with_context(vectorstore, query, k=10):
    """
    Recherche en 2 étapes :
    1. Posts pertinents
    2. Replies associées
    """
    # Étape 1 : Posts principaux
    posts = vectorstore.similarity_search_with_score(
        query=query,
        k=k,
        filter={"doc_type": "original_post"},
    )

    results = []
    seen = set()

    for doc, score in posts:
        # CHANGEMENT : post_id depuis metadata, plus depuis page_content
        post_id = doc.metadata.get("post_id", "")

        entry = {
            "post": doc,
            "score": float(score),
            "post_id": post_id,
            "replies": [],
        }

        # Étape 2 : Replies de ce post
        if post_id and post_id not in seen:
            try:
                replies = vectorstore.similarity_search(
                    query=query,
                    k=5,
                    filter={"parent_post_id": post_id},
                )
                entry["replies"] = replies
            except Exception:
                pass
            seen.add(post_id)

        results.append(entry)

    return results


def format_context(results, max_results=5):
    """
    Formate pour le prompt du LLM.
    Reconstruit le texte structuré depuis metadata.
    """
    blocks = []

    for i, r in enumerate(results[:max_results]):
        meta = r["post"].metadata
        content = r["post"].page_content

        # Reconstruire le texte structuré
        post_id = meta.get("post_id", "?")
        channel = meta.get("channel_name", "?")
        doc_type = meta.get("doc_type", "?")

        block = f"══ SOURCE {i+1} "
        block += f"(pertinence: {r['score']:.3f}) ══\n"
        block += (
            f"[POST_ID: {post_id}] | "
            f"CHANNEL: {channel} | "
            f"CONTENT: {content}\n"
        )

        # Métadonnées utiles
        views = meta.get("views", "")
        forwards = meta.get("forwards", "")
        if views:
            block += (
                f"  [Views: {views} | "
                f"Forwards: {forwards}]\n"
            )

        # Replies
        if r["replies"]:
            block += "  ── Réactions communauté ──\n"
            for reply in r["replies"]:
                r_meta = reply.metadata
                r_id = r_meta.get("reply_id", "?")
                block += (
                    f"  → [REPLY_ID: {r_id}] "
                    f"{reply.page_content}\n"
                )

        blocks.append(block)

    return "\n\n".join(blocks)


# ══════════════════════════════════════════════
# PROMPTS CTI
# ══════════════════════════════════════════════

REWRITE_PROMPT = ChatPromptTemplate.from_messages([
    ("system",
     "Tu es un analyste CTI. Reformule cette question en "
     "requête optimisée pour la recherche dans une base de "
     "posts Telegram cybercriminels. Ajoute des termes "
     "techniques CTI. Max 25 mots. Réponds UNIQUEMENT "
     "avec la requête."),
    ("human", "{question}")
])

ANALYSIS_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """Tu es un analyste senior CTI.
Analyse les données suivantes extraites de canaux
Telegram cybercriminels (dataset DarkGram).

DONNÉES RÉCUPÉRÉES :
{context}

RÈGLES :
1. Base-toi UNIQUEMENT sur les données ci-dessus
2. Cite les POST_ID et CHANNEL dans tes sources
3. Évalue la fiabilité :
   - Posts avec beaucoup de replies interrogatives
     = probable spam/scam
   - Posts avec views élevées + forwards = menace
     potentiellement réelle
   - Posts RECOVERED ont moins de métadonnées
4. Si les données sont insuffisantes, dis-le

FORMAT :
## Analyse
[Ton analyse factuelle]

## Indicateurs de Menace (IOC)
- [URLs, domaines, outils mentionnés]

## Sources
- POST_ID: X | Channel: Y | Fiabilité: Z

## Fiabilité Globale
[Élevée/Moyenne/Faible] - [justification]"""),
    ("human", "{question}")
])


# ══════════════════════════════════════════════
# AGENT CTI
# ══════════════════════════════════════════════

class CTIAgent:
    def __init__(self, vectorstore):
        self.vectorstore = vectorstore
        self.llm = get_llm()
        self.parser = StrOutputParser()
        self.rewrite_chain = (
            REWRITE_PROMPT | self.llm | self.parser
        )
        self.analysis_chain = (
            ANALYSIS_PROMPT | self.llm | self.parser
        )

    def analyze(self, question, k=10, verbose=True):
        """Pipeline RAG complet."""

        # 1. Reformulation
        if verbose:
            print(f"\n🔍 Question : {question}")

        rewritten = self.rewrite_chain.invoke(
            {"question": question}
        )
        if verbose:
            print(f"🔄 Reformulée : {rewritten}")

        # 2. Retrieval avec contexte
        results = retrieve_with_context(
            self.vectorstore, rewritten, k=k
        )
        if verbose:
            print(f"📦 {len(results)} résultats trouvés")

        # 3. Formatage
        context = format_context(results, max_results=5)
        if verbose:
            print(f"📋 Contexte : {len(context)} car.")

        # 4. Analyse
        if verbose:
            print(f"🤖 Analyse en cours...")

        analysis = self.analysis_chain.invoke({
            "context": context,
            "question": question,
        })

        return {
            "question": question,
            "rewritten": rewritten,
            "analysis": analysis,
            "sources": [
                {
                    "post_id": r["post_id"],
                    "score": r["score"],
                    "replies": len(r["replies"]),
                    "channel": r["post"].metadata.get(
                        "channel_name", ""
                    ),
                }
                for r in results[:5]
            ],
        }