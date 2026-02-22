import requests

def scraper_stackoverflow():
    url = "https://api.stackexchange.com/2.3/questions"
    
    # Paramètres de la recherche
    params = {
        "order"  : "desc",
        "sort"   : "creation",
        "tagged" : "security",
        "site"   : "stackoverflow",
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
            "tags"  : item["tags"],
        })
    
    return questions


# ── Tester ──
questions = scraper_stackoverflow()

for q in questions:
    print(f"[{q['source']}] {q['titre']}")
    print(f"  Lien → {q['lien']}")
    print(f"  Tags → {q['tags']}")
    print()