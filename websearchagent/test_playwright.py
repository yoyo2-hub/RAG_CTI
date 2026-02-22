import requests

def scraper_reddit():
    url = "https://www.reddit.com/r/netsec/hot.json?limit=5"
    
    # Simuler un vrai navigateur
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    response = requests.get(url, headers=headers)
    data = response.json()
    
    posts = []
    for post in data["data"]["children"]:
        info = post["data"]
        posts.append({
            "source" : "Reddit r/netsec",
            "titre"  : info["title"],
            "lien"   : info["url"],
            "contenu": info["selftext"][:300],
        })
    
    return posts


# ── Tester ──
posts = scraper_reddit()

for p in posts:
    print(f"[{p['source']}] {p['titre']}")
    print(f"  Lien → {p['lien']}")
    print()