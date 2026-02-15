import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

# --- CHARGEMENT & VALIDATION ---
data = []
errors = 0
print("🔍 Chargement du fichier JSONL...")

with open('darkgram_cti_final_granular.jsonl', 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        try:
            data.append(json.loads(line))
        except Exception as e:
            errors += 1
            print(f"❌ Erreur ligne {i}: {e}")

if errors == 0:
    print("✅ Intégrité du fichier : OK (aucune erreur de parsing)")
else:
    print(f"⚠️ {errors} lignes corrompues détectées.")

# --- EXTRACTION VERS DATAFRAME ---
# On utilise .get() pour éviter les erreurs si une clé manque dans les Replies
rows = []
for d in data:
    meta = d.get('metadata', {})
    rows.append({
        'category': meta.get('category', 'Unknown'),
        'doc_type': meta.get('doc_type', 'Unknown'),
        'channel': meta.get('channel_name') or meta.get('channel', 'Unknown'),
        'replies_count': meta.get('replies_count', 0) if meta.get('doc_type') == 'original_post' else 0,
        'text_len': len(d.get('text', '')),
        'is_recovered': meta.get('recovered', False)
    })

df = pd.DataFrame(rows)

# --- CONFIGURATION DU DASHBOARD ---
sns.set_theme(style="whitegrid")
fig, axes = plt.subplots(2, 2, figsize=(18, 12))
plt.subplots_adjust(hspace=0.4, wspace=0.3)

# 1. DISTRIBUTION DES CATÉGORIES
axes[0, 0].set_title("📦 Répartition des Menaces", fontsize=14, fontweight='bold')
df['category'].value_counts().plot.pie(autopct='%1.1f%%', ax=axes[0, 0], colors=sns.color_palette("viridis"))
axes[0, 0].set_ylabel('')

# 2. TOP 10 DES CANAUX
axes[0, 1].set_title("📢 Canaux les plus bavards (Posts + Replies)", fontsize=14, fontweight='bold')
top_channels = df['channel'].value_counts().nlargest(10)
sns.barplot(x=top_channels.values, y=top_channels.index, ax=axes[0, 1], palette="magma", hue=top_channels.index, legend=False)
axes[0, 1].set_xlabel("Nombre de Chunks JSONL")

# 3. TYPE DE DOCUMENTS (Posts vs Replies)
axes[1, 0].set_title("📊 Granularité des Données", fontsize=14, fontweight='bold')
sns.countplot(x='doc_type', data=df, ax=axes[1, 0], palette="coolwarm", hue='doc_type', legend=False)
axes[1, 0].set_xlabel("Type de Document")
axes[1, 0].set_ylabel("Quantité")

# 4. ANALYSE DE LA LONGUEUR (Distribution)
axes[1, 1].set_title("📏 Taille des Chunks (Texte)", fontsize=14, fontweight='bold')
sns.histplot(df['text_len'], bins=50, ax=axes[1, 1], color="teal", kde=True)
axes[1, 1].set_xlabel("Nombre de caractères")

# Sauvegarde
plt.savefig('cti_dashboard_analyse.png', dpi=300)

# --- STATISTIQUES CONSOLE ---
print("\n" + "="*30)
print("       RÉSUMÉ STATISTIQUE")
print("="*30)
print(f"🔹 Total des documents : {len(df)}")
print(f"🔹 Posts originaux    : {len(df[df['doc_type'] == 'original_post'])}")
print(f"🔹 Réponses (Replies) : {len(df[df['doc_type'] == 'reply'])}")
print(f"🔹 Posts récupérés    : {df['is_recovered'].sum()}")
print(f"🔹 Longueur moyenne   : {df['text_len'].mean():.0f} caractères")

# Top Mots-Clés avec Counter (plus efficace)
keywords = ['cookie', 'leak', 'premium', 'account', 'login', 'free', 'vpn', 'method', 'scam', 'stealer']
text_blob = " ".join([d['text'].lower() for d in data])
kw_counts = Counter()
for kw in keywords:
    kw_counts[kw] = text_blob.count(kw)

print("\n--- TOP MOTS-CLÉS TECHNIQUES ---")
for kw, count in kw_counts.most_common():
    print(f"🔑 {kw.upper().ljust(10)} : {count}")

print("\n✅ Analyse terminée. Visualisez 'cti_dashboard_analyse.png'")