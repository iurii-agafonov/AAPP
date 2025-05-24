from flask import Flask, jsonify
import pandas as pd
from tqdm import tqdm
from datetime import datetime, date
import re
import json
import gdrive
import pytz
import joblib
import numpy as np
from sentence_transformers import SentenceTransformer
import spacy
import os

app = Flask(__name__)

#############################
#SETUP PART
#############################
st_model = SentenceTransformer(os.getenv("MODEL_PATH", "/app/models/all-MiniLM-L6-v2"))
classifier_arm = joblib.load(os.getenv("CLASSIFIER_ARM_PATH", "/app/models/classifier_arm.joblib"))
classifier_aze = joblib.load(os.getenv("CLASSIFIER_AZE_PATH", "/app/models/classifier_aze.joblib"))
nlp = spacy.load(os.getenv("SPACY_MODEL_PATH", "/app/models/en_core_web_sm/en_core_web_sm-3.8.0"))

keywords = [
    "delimitation", "regional security", "normalization", "normalization process", "alma-ata declaration", "lachin corridor",
    "regional stability", "peace treaty", "armenia-azerbaijan relations",
    "crossroads of peace", "crossroad of peace", "regional peace",
    "peace project", "osce minsk group", "peace agenda", "trilateral statement",
    "regional communication", "peace and stability", "demarcation", "peace agreement",
    "peace process", "delimitation process", "crossroads", "regional peace", "de-escalation", "regional stability", "stability in the region",
    "peace and stability in the region", "peace-building efforts", "peace in the region", "post-conflict situation", "lasting peace and security in the region",
    "lasting peace",
    "trilateral working group", "international traffic", "unblocking", "normalizing relations", "regional peace-building"
    ]


def classify_texts (df, model, classifier):
    df = df.dropna(subset=['full_text_eng'])
    df = df[df['full_text_eng'].str.strip() != '']
    df["id"] = df["id"].astype(str)

    # 1. Compute embeddings for all texts in the entire dataset.
    all_texts = df["full_text_eng"].tolist()
    X_all_arm = model.encode(
        all_texts,
        batch_size=16,                 # Adjust to fit your memory (8, 16, or 32)
        convert_to_numpy=True,
        show_progress_bar=False        # Set to True locally to debug
    )
    # 2. Use the trained logistic regression classifier to predict labels.
    #    This uses the transformer-based classifier only.
    all_preds_arm = classifier.predict(X_all_arm)

    # 3. Optionally, convert numeric predictions back to string labels.
    #    For example, if 0 corresponds to "no" and 1 corresponds to "yes":
    df["predicted_label"] = np.where(all_preds_arm == 1, "yes", "no")

    return df

def determine_final_label(row):
    text = row['full_text_eng'].lower()
    # Check if any keyword is in the text
    if any(kw in text for kw in keywords):
        return "yes"
    else:
        return "no"
    
# Function to lemmatize text
def lemmatize_text(text):
    doc = nlp(text)
    return " ".join([token.lemma_ for token in doc])


@app.route("/", methods=["POST"])
def run_classification():
    try:
        folder_id = "1GbM7j6NwoItCRPDoCHxLBv2ld2pYnnUj"
        file_arm_new = "ArmEng_complete1.json"
        file_aze_new = "AzeEng_complete1.json"
        folder_id_old = "1xUwgk_bt4UyRpEUZTwuFMNsoYbu8TGLH"
        file_arm_old = "arm_lemm.json"
        file_aze_old = "aze_lemm.json"

        df_arm = gdrive.read_json_from_drive(folder_id, file_arm_new)
        df_aze = gdrive.read_json_from_drive(folder_id, file_aze_new)

        df_arm_old = gdrive.read_json_from_drive(folder_id_old, file_arm_old)
        df_aze_old = gdrive.read_json_from_drive(folder_id_old, file_aze_old)

        df_arm = pd.DataFrame(df_arm)
        df_aze = pd.DataFrame(df_aze)

        df_arm = classify_texts(df_arm, st_model, classifier_arm)
        df_aze = classify_texts(df_aze, st_model, classifier_aze)

        df_arm["final_label"] = df_arm.apply(determine_final_label, axis=1)
        df_aze["final_label"] = df_aze.apply(determine_final_label, axis=1)

        df_arm["lemmatized_text"] = df_arm["full_text_eng"].apply(lemmatize_text)
        df_aze["lemmatized_text"] = df_aze["full_text_eng"].apply(lemmatize_text)

        df_arm_new = df_arm[df_arm["final_label"] == "yes"]
        df_aze_new = df_aze[df_aze["final_label"] == "yes"]

        arm_new_records = json.loads(df_arm_new.replace({np.nan: None, pd.NaT: None}).to_json(orient="records", force_ascii=False))
        aze_new_records = json.loads(df_aze_new.replace({np.nan: None, pd.NaT: None}).to_json(orient="records", force_ascii=False))

        df_arm_old.extend(arm_new_records)
        df_aze_old.extend(aze_new_records)

        json_data_arm = json.dumps(df_arm_old, ensure_ascii=False, indent=4)
        json_data_aze = json.dumps(df_aze_old, ensure_ascii=False, indent=4)

        gmt_now = datetime.now(pytz.utc)
        target_tz = pytz.timezone('Asia/Yerevan')
        local_time = gmt_now.astimezone(target_tz)
        new_time = local_time.strftime("%d_%m_%Y")

        gdrive.save_json_to_drive(json_data_arm, folder_id_old, f"arm_lemm_{new_time}.json")
        gdrive.save_json_to_drive(json_data_aze, folder_id_old, f"aze_lemm_{new_time}.json")

        return jsonify({"message": "Success!"}), 200

    except Exception as e:
        # Log the error, return error message
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Optional local run for debugging
    app.run(host="0.0.0.0", port=8080)