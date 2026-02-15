import pandas as pd
import json
import os
import re

# --- CONFIGURATION ---
DATA_ROOT = 'cleaned_data' 
OUTPUT_FILE = 'darkgram_cti_final_granular.jsonl'

def clean_val(val):
    """Convertit les types NumPy/Pandas en types Python natifs pour le JSON."""
    if pd.isna(val): 
        return None
    # .item() convertit int64/float64 en int/float Python natif
    if hasattr(val, 'item'): 
        return val.item()
    return val

def id_to_str(val):
    """Force la conversion de l'ID en string propre (sans .0)."""
    if pd.isna(val): return ""
    try:
        # Gère les cas où l'ID est lu comme un float (ex: 123.0)
        return str(int(float(val)))
    except:
        return str(val).strip()

def process_to_jsonl():
    total_posts = 0
    total_replies = 0
    total_recovered = 0
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        if not os.path.exists(DATA_ROOT):
            print(f"Erreur : Le dossier {DATA_ROOT} n'existe pas.")
            return

        for category in os.listdir(DATA_ROOT):
            cat_path = os.path.join(DATA_ROOT, category)
            if not os.path.isdir(cat_path): continue

            for channel_name in os.listdir(cat_path):
                channel_path = os.path.join(cat_path, channel_name)
                posts_csv = os.path.join(channel_path, f"{channel_name}.csv")
                replies_folder = os.path.join(channel_path, f"{channel_name}_replies")
                
                processed_ids = set() 

                # --- PHASE 1 : POSTS ---
                if os.path.exists(posts_csv):
                    try:
                        df_posts = pd.read_csv(posts_csv, low_memory=False)
                        df_posts.rename(columns=lambda x: x.strip().lower(), inplace=True)

                        for _, post in df_posts.iterrows():
                            p_id = id_to_str(post.get('post id'))
                            if not p_id: continue

                            text_formatted = f"POST_ID: {p_id}\nTYPE: MAIN_POST\nCHANNEL: {channel_name}\nCONTENT:\n{post.get('message', '')}\nDATE: {post.get('date', 'Unknown')}"
                            
                            metadata = {col: clean_val(post.get(col)) for col in df_posts.columns}
                            metadata.update({"category": category, "doc_type": "original_post", "post_id": p_id})

                            f_out.write(json.dumps({"text": text_formatted, "metadata": metadata}, ensure_ascii=False) + '\n')
                            processed_ids.add(p_id)
                            total_posts += 1
                    except Exception as e:
                        print(f"⚠️ Erreur CSV Posts {channel_name}: {e}")

                # --- PHASE 2 : REPLIES & RECOVERY ---
                if os.path.exists(replies_folder):
                    for f_rep in os.listdir(replies_folder):
                        if not f_rep.endswith("_replies.csv"): continue
                        
                        # Extraction robuste de l'ID (ex: 'timestamp_ID_replies.csv' -> on prend l'ID)
                        parts = f_rep.replace("_replies.csv", "").split("_")
                        p_id_from_file = parts[-1] if parts else None

                        if not p_id_from_file: continue

                        try:
                            df_rep = pd.read_csv(os.path.join(replies_folder, f_rep), low_memory=False)
                            df_rep.rename(columns=lambda x: x.strip().lower(), inplace=True)
                            if df_rep.empty: continue

                            # AUTO-RECOVERY DU POST SI ABSENT DU CSV PRINCIPAL
                            if p_id_from_file not in processed_ids:
                                row_zero = df_rep.iloc[0]
                                text_rec = f"POST_ID: {p_id_from_file}\nTYPE: MAIN_POST \nCHANNEL: {channel_name}\nCONTENT:\n{row_zero.get('message', '')}\nDATE: {row_zero.get('date', 'Unknown')}"
                                
                                meta_rec = {col: clean_val(row_zero.get(col)) for col in df_rep.columns}
                                meta_rec.update({"category": category, "doc_type": "original_post", "recovered": True, "post_id": p_id_from_file})
                                
                                f_out.write(json.dumps({"text": text_rec, "metadata": meta_rec}, ensure_ascii=False) + '\n')
                                processed_ids.add(p_id_from_file)
                                total_recovered += 1
                                total_posts += 1

                            # TRAITEMENT DES RÉPONSES
                            for _, rep in df_rep.iloc[1:].iterrows():
                                r_id = id_to_str(rep.get('id'))
                                text_reply = f"[PARENT_POST_ID: {p_id_from_file}] [REPLY_ID: {r_id}] TYPE: REPLY | CHANNEL: {channel_name} | CONTENT: {rep.get('message', '')}"
                                
                                meta_reply = {col: clean_val(rep.get(col)) for col in df_rep.columns}
                                meta_reply.update({"category": category, "doc_type": "reply", "parent_post_id": p_id_from_file, "reply_id": r_id})

                                f_out.write(json.dumps({"text": text_reply, "metadata": meta_reply}, ensure_ascii=False) + '\n')
                                total_replies += 1
                        except Exception as e:
                            print(f"⚠️ Erreur fichier {f_rep}: {e}")

    print(f"\n✅ Terminé !")
    print(f"   - Posts (Total): {total_posts} (dont {total_recovered} récupérés)")
    print(f"   - Réponses: {total_replies}")

if __name__ == "__main__":
    process_to_jsonl()