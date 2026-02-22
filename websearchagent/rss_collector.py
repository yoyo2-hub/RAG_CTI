import requests
import feedparser
OTX_API_KEY = "1552be520d74f388e1f0e7349c76a351020ecf8cdee408d2d8284350547307df"
VT_API_KEY  = "033bb1a6ed107c16be58dc1816fab8704b67ecadc68c0aca6a91128a8e7314ee"

# ─────────────────────────────────────
# 1.1 RSS
# ─────────────────────────────────────
def collecter_rss():
    SOURCES = {
        "TheHackerNews"  : "https://feeds.feedburner.com/TheHackersNews",
        "BleepingComputer": "https://www.bleepingcomputer.com/feed/",
    }
    articles = []
    for nom, url in SOURCES.items():
        feed = feedparser.parse(url)
        for article in feed.entries[:5]:
            articles.append({
                "source" : nom,
                "titre"  : article.title,
                "lien"   : article.link,
                "resume" : article.summary,
            })
    return articles

# ─────────────────────────────────────
# 1.2 SCRAPERS
# ─────────────────────────────────────
def collecter_reddit():
    url = "https://www.reddit.com/r/netsec/hot.json?limit=5"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    response = requests.get(url, headers=headers)
    data = response.json()
    posts = []
    for post in data["data"]["children"]:
        info = post["data"]
        posts.append({
            "source" : "Reddit r/netsec",
            "titre"  : info["title"],
            "lien"   : info["url"],
            "resume" : info["selftext"][:300],
        })
    return posts

def collecter_stackoverflow():
    url = "https://api.stackexchange.com/2.3/questions"
    params = {
        "order"   : "desc",
        "sort"    : "creation",
        "tagged"  : "security",
        "site"    : "stackoverflow",
        "pagesize": 5,
    }
    response = requests.get(url, params=params)
    data = response.json()
    questions = []
    for item in data["items"]:
        questions.append({
            "source": "StackOverflow",
            "titre" : item["title"],
            "lien"  : item["link"],
            "resume": str(item["tags"]),
        })
    return questions

# ─────────────────────────────────────
# 1.3 APIs CTI
# ─────────────────────────────────────
def collecter_otx():
    url = "https://otx.alienvault.com/api/v1/pulses/subscribed"
    headers = {"X-OTX-API-KEY": OTX_API_KEY}
    response = requests.get(url, headers=headers)
    data = response.json()
    menaces = []
    for pulse in data.get("results", [])[:5]:
        menaces.append({
            "source"     : "AlienVault OTX",
            "titre"      : pulse["name"],
            "lien"       : f"https://otx.alienvault.com/pulse/{pulse['id']}",
            "resume"     : pulse["description"][:300],
        })
    return menaces

# ─────────────────────────────────────
# TOUT ASSEMBLER
# ─────────────────────────────────────
def collecter_tout():
    print("📡 Collecte RSS...")
    rss = collecter_rss()

    print("🕷️  Collecte Reddit...")
    reddit = collecter_reddit()

    print("🕷️  Collecte StackOverflow...")
    stackoverflow = collecter_stackoverflow()

    print("🔍 Collecte AlienVault OTX...")
    otx = collecter_otx()

    tous = rss + reddit + stackoverflow + otx

    print(f"\n✅ Total collecté : {len(tous)} éléments")
    print(f"   → RSS           : {len(rss)}")
    print(f"   → Reddit        : {len(reddit)}")
    print(f"   → StackOverflow : {len(stackoverflow)}")
    print(f"   → OTX           : {len(otx)}")

    return tous

# ── Tester ──
if __name__ == "__main__":
    donnees = collecter_tout()
    print("\n── Aperçu des données ──")
    for d in donnees:
        print(f"[{d['source']}] {d['titre']}")