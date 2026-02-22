import requests

# ─────────────────────────────────────
# TES CLÉS API
# ─────────────────────────────────────
OTX_API_KEY = "1552be520d74f388e1f0e7349c76a351020ecf8cdee408d2d8284350547307df"
VT_API_KEY  = "033bb1a6ed107c16be58dc1816fab8704b67ecadc68c0aca6a91128a8e7314ee"


# ─────────────────────────────────────
# 1. ALIENVAULT OTX
# ─────────────────────────────────────
def otx_get_menaces():
    url = "https://otx.alienvault.com/api/v1/pulses/subscribed"
    headers = {"X-OTX-API-KEY": OTX_API_KEY}
    
    response = requests.get(url, headers=headers)
    data = response.json()
    
    menaces = []
    for pulse in data.get("results", [])[:5]:
        menaces.append({
            "source"     : "AlienVault OTX",
            "nom"        : pulse["name"],
            "description": pulse["description"][:300],
            "tags"       : pulse["tags"],
        })
    
    return menaces


# ─────────────────────────────────────
# 2. VIRUSTOTAL
# ─────────────────────────────────────
def vt_analyser_url(url_cible: str):
    url = "https://www.virustotal.com/api/v3/urls"
    headers = {"x-apikey": VT_API_KEY}
    
    # Soumettre l'URL
    response = requests.post(url, headers=headers, data={"url": url_cible})
    data = response.json()
    
    # Récupérer l'ID d'analyse
    analyse_id = data["data"]["id"]
    
    # Récupérer le résultat
    result_url = f"https://www.virustotal.com/api/v3/analyses/{analyse_id}"
    result = requests.get(result_url, headers=headers).json()
    
    stats = result["data"]["attributes"]["stats"]
    return {
        "source"      : "VirusTotal",
        "url"         : url_cible,
        "malveillant" : stats["malicious"],
        "suspect"     : stats["suspicious"],
        "propre"      : stats["harmless"],
    }


# ─────────────────────────────────────
# TESTER
# ─────────────────────────────────────
if __name__ == "__main__":

    # Test OTX
    print("🔍 AlienVault OTX — Menaces récentes...")
    menaces = otx_get_menaces()
    for m in menaces:
        print(f"  [{m['source']}] {m['nom']}")
        print(f"   Tags → {m['tags']}")
        print()

    # Test VirusTotal
    print("🔍 VirusTotal — Analyse URL...")
    resultat = vt_analyser_url("https://www.google.com")
    print(f"  URL         : {resultat['url']}")
    print(f"  Malveillant : {resultat['malveillant']}")
    print(f"  Suspect     : {resultat['suspect']}")
    print(f"  Propre      : {resultat['propre']}")