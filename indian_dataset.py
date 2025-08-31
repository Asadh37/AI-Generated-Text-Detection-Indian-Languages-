import itertools
import pandas as pd
from datasets import load_dataset

#Hugging Face Repo Link :- https://huggingface.co/datasets/ai4bharat/IndicCorpV2

def sample_language(lang, n=50000):
    ds = load_dataset("ai4bharat/IndicCorpV2", "indiccorp_v2", split=lang, streaming=True)
    return list(itertools.islice(ds, n))


langs = ["hin_Deva", "tel_Telu", "kan_Knda", "tam_Taml", "mal_Mlym", "ben_Beng"] #Add Needed New Languages

for lang in langs:
    print(f"Sampling {lang} ...")
    samples = sample_language(lang, n=50000)


    df = pd.DataFrame(samples)

    save_path = f"{lang}_done_local.jsonl"
    df.to_json(save_path, orient="records", lines=True, force_ascii=False)
    print(f"✅ Saved {save_path}")