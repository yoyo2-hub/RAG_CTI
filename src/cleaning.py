import pandas as pd
import os
import re

INPUT_DIR = "data"
OUTPUT_DIR = "cleaned_data"

def selective_clean(text):
    if pd.isna(text) or not isinstance(text, str):
        return None

    # Normaliser tous les espaces (tab, newline, espaces multiples)
    text = re.sub(r'\s+', ' ', text)

    # Réduire décorations excessives
    text = re.sub(r'([*\-=_~@#$%&]){2,}', r'\1', text)
    # Réduire ponctuation répétitive (., !, ?, …)
    text = re.sub(r'([.!?]){2,}', r'\1', text)
    # Réduire les emojis excessives
    emoji_pattern = re.compile(
        r'([\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF\U0001F600-\U0001F64F])\1+'
    )
    text = emoji_pattern.sub(r'\1', text)


    text = text.strip()

    if text == "":
        return None

    return text


def run_cleaning(input_dir, output_dir):
    print(f"🧹 Nettoyage depuis '{input_dir}' vers '{output_dir}'")

    total_files = 0
    total_deleted = 0

    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".csv"):
                input_path = os.path.join(root, file)

                # Recréer la structure du dossier
                relative_path = os.path.relpath(root, input_dir)
                output_folder = os.path.join(output_dir, relative_path)
                os.makedirs(output_folder, exist_ok=True)

                output_path = os.path.join(output_folder, file)

                try:
                    df = pd.read_csv(input_path, low_memory=False)

                    # Trouver colonne message
                    col_name = None
                    for col in df.columns:
                        if col.lower() in ["message", "text", "content"]:
                            col_name = col
                            break

                    if not col_name:
                        print(f"⚠️ Colonne message non trouvée dans {file}")
                        continue

                    original_len = len(df)

                    # Nettoyage texte
                    df[col_name] = df[col_name].apply(selective_clean)

                    # Supprimer les doublons exacts sur message + Channel ID + Out + Silent + Edit_Hide
                    dup_cols = [col_name, 'Channel ID']
                    df = df.drop_duplicates(subset=dup_cols, keep='last')

                    # Supprimer uniquement les lignes vides
                    df.dropna(subset=[col_name], inplace=True)
                    
                    
                    
                    deleted = original_len - len(df)
                    total_deleted += deleted

                    # Sauvegarde
                    df.to_csv(output_path, index=False, encoding="utf-8")

                    print(f"✅ {file} : {deleted} lignes vides supprimées")

                    total_files += 1

                except Exception as e:
                    print(f"⚠️ Erreur sur {file}: {e}")

    print("✨ Nettoyage terminé")
    print(f"📂 Fichiers traités : {total_files}")
    print(f"🗑️ Lignes vides supprimées au total : {total_deleted}")


if __name__ == "__main__":
    run_cleaning(INPUT_DIR, OUTPUT_DIR)
