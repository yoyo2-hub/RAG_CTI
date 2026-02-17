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
# VALIDATION
# ══════════════════════════════════════════════

def is_relevant_question(question):
    """Rejette les questions hors-sujet CTI."""
    question_lower = question.lower().strip()

    irrelevant_patterns = [
        r'^(hi|hello|hey|bonjour|salut|coucou)',
        r'^how are you',
        r'^(merci|thanks|thank you)',
        r'^(bye|goodbye|au revoir)',
        r'^(oui|non|yes|no|ok|okay)$',
        r'^(test|testing)$',
        r'^what is the weather',
        r'^who are you',
        r'^what can you do',
    ]

    for pattern in irrelevant_patterns:
        if re.match(pattern, question_lower):
            return False

    if len(question_lower.split()) < 3:
        return False

    return True


# ══════════════════════════════════════════════
# RETRIEVER INTELLIGENT
# ══════════════════════════════════════════════

# Seuil de pertinence : au-dessus = non pertinent
RELEVANCE_THRESHOLD = 0.75


def retrieve_with_context(vectorstore, query, k=10):
    """
    Recherche en 2 étapes avec FILTRAGE par score.
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
        # FILTRAGE : ignorer les résultats non pertinents
        if score > RELEVANCE_THRESHOLD:
            continue

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
                    filter={"parent_post_id": str(post_id)},
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
    Reconstruit le contexte depuis metadata.
    """
    if not results:
        return "AUCUN RÉSULTAT PERTINENT TROUVÉ."

    blocks = []

    for i, r in enumerate(results[:max_results]):
        meta = r["post"].metadata
        content = r["post"].page_content
        post_id = meta.get("post_id", "?")
        channel = meta.get("channel_name", "?")

        block = f"══ SOURCE {i+1} "
        block += f"(score: {r['score']:.3f}) ══\n"
        block += (
            f"[POST_ID: {post_id}] | "
            f"CHANNEL: {channel} | "
            f"CONTENT: {content}\n"
        )

        views = meta.get("views", "")
        forwards = meta.get("forwards", "")
        if views:
            block += (
                f"  [Views: {views} | "
                f"Forwards: {forwards}]\n"
            )

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
     "avec la requête reformulée, rien d'autre."),
    ("human", "{question}")
])

ANALYSIS_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """Tu es un analyste senior CTI.
Analyse les données suivantes extraites de canaux
Telegram cybercriminels (dataset DarkGram).

DONNÉES RÉCUPÉRÉES :
{context}

RÈGLES STRICTES :
1. Base-toi UNIQUEMENT sur les données ci-dessus
2. Ne JAMAIS inventer des informations absentes du contexte
3. Cite les POST_ID et CHANNEL exacts dans tes sources
4. Si le contexte dit "AUCUN RÉSULTAT PERTINENT",
   réponds que tu n'as pas trouvé de données pertinentes
5. Évalue la fiabilité :
   - Posts avec replies interrogatives = probable spam/scam
   - Posts avec views élevées + forwards = menace potentielle
   - Score de pertinence < 0.3 = très pertinent
   - Score de pertinence > 0.5 = peu pertinent
6. Ne fais PAS d'analyse si les données sont insuffisantes

FORMAT :
## Analyse
[Ton analyse factuelle basée sur les données]

## Indicateurs de Menace (IOC)
- [UNIQUEMENT ceux présents dans le contexte]

## Sources
- POST_ID: X | Channel: Y | Score: Z

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
        """Pipeline RAG complet avec validation."""

        # Vérification pertinence
        if not is_relevant_question(question):
            msg = (
                "⚠️ Cette question ne semble pas liée "
                "à la Cyber Threat Intelligence.\n\n"
                "Exemples de questions valides :\n"
                "- What cracking tools are shared?\n"
                "- Which channels sell stolen credentials?\n"
                "- What cloud logs are available?\n"
                "- Are there pirated software shared?"
            )
            if verbose:
                print(f"\n⚠️ Question hors-sujet détectée")
            return {
                "question": question,
                "rewritten": None,
                "analysis": msg,
                "sources": [],
            }

        # 1. Reformulation
        if verbose:
            print(f"\n🔍 Question : {question}")

        rewritten = self.rewrite_chain.invoke(
            {"question": question}
        )
        if verbose:
            print(f"🔄 Reformulée : {rewritten}")

        # 2. Retrieval
        results = retrieve_with_context(
            self.vectorstore, rewritten, k=k
        )
        if verbose:
            print(f"📦 {len(results)} résultats pertinents")

        # Aucun résultat pertinent
        if not results:
            if verbose:
                print("❌ Aucun résultat sous le seuil")
            return {
                "question": question,
                "rewritten": rewritten,
                "analysis": (
                    "❌ Aucun résultat pertinent trouvé "
                    "dans la base DarkGram.\n"
                    "Les documents trouvés avaient un score "
                    "de similarité trop faible "
                    f"(seuil: {RELEVANCE_THRESHOLD})."
                ),
                "sources": [],
            }

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