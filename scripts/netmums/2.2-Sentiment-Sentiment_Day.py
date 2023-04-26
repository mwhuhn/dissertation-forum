import pandas as pd
import numpy as np
from pathlib import Path
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from scipy.special import softmax
from torch import tensor


model_name = "cardiffnlp/twitter-roberta-base-emotion"

model = AutoModelForSequenceClassification.from_pretrained(model_name)

tokenizer = AutoTokenizer.from_pretrained(model_name)

p = Path.cwd()
path_parent = p.parents[1]
path_clean_data = path_parent / "clean_data" / "netmums"
path_text_pkl = str(path_clean_data / "daily_clean_text.pkl")
path_emote_text_pkl = str(path_clean_data / "daily_emote_clean_text_{}.pkl")

df = pd.read_pickle(path_text_pkl)
more_than_one = df.groupby("user_url")['day'].count()
more_than_one = more_than_one[more_than_one > 1].reset_index(drop=False)
df = df.loc[df['user_url'].isin(more_than_one['user_url'])]

df = df.iloc[0:1000000]


def split_text(text, max_size):
    """ 
    """
    # filter long words
    text = " ".join([word for word in text.split(" ") if len(word) <=25])
    # tokenize text
    encoded = tokenizer.encode(text)[1:-1]
    n_tokens = len(encoded)
    if n_tokens >= max_size:
        n_chunks = n_tokens // max_size + (n_tokens % max_size > 0) # round up
        chunk_size = n_words // n_chunks + (n_tokens % n_chunks > 0)
        return [[0] + tokenizer.decode(encoded[i:i + chunk_size]) + [2] for i in range(0, n_tokens, chunk_size)]
    else:
        return [[0] + tokenizer.decode(split_text_list) + [2]]


ddf = dd.from_pandas(df, npartitions=200)
ddf['split'] = ddf.apply(lambda x: split_text(x['text'], 300), axis=1, meta=list)
df = ddf.compute(scheduler='processes')

text = "I'm so happy right now 😎"
text = " ".join([word for word in text.split(" ") if len(word) <=25])
encoded_input = tokenizer(text, return_tensors='pt')
encoded_input = {'input_ids': tensor()}

encoded = tokenizer.encode(text)
decoded = tokenizer.decode(encoded[1:-1])
encoded_input = tokenizer(decoded, return_tensors='pt')
output = model(**encoded_input)
scores = output[0][0].detach().numpy()
scores = softmax(scores)
scores


def get_n_tokens(text):
    tokens = tokenizer.encode(text)
    return len(tokens)

def make_tensor(encoded_list):
    output = {
        'input_ids': tensor([encoded_list]),
        'attention_mask': tensor([[1 for i in encoded_list]])
    }
    return output

tensor_input = make_tensor(encoded_list)
output = model(**tensor_input)
scores = output[0][0].detach().numpy()
scores = softmax(scores)
scores

encoded_input = tokenizer("I'm so happy right", return_tensors='pt')


def get_emotion_scores(encoded_tensor):
    output = model(**encoded_input)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    return scores

