import itertools as it
from collections import Counter

import pandas as pd

DENYLIST = ["china", "russia", "turkey", "op"]


def get_ents(df):
    """Add an entities column to the given df."""
    df["ents"] = df["spacy"].apply(lambda sp: list(sp.ents) if (sp and len(sp.ents) > 0) else None)
    return df.dropna()


def df_by_ent(df, e_name):
    """subset an entity df by the given ent."""
    sub_df = df.copy()
    sub_df[e_name] = sub_df["ents"].apply(lambda ents: [e for e in ents if e.label_ == e_name])
    sub_df[e_name] = sub_df[e_name].apply(lambda ents: ents if len(ents) > 0 else None)
    return sub_df.dropna()


def group_gpes(df):
    """Group all gpe by user."""

    def agg_gpe(gpes):
        return dict(Counter(it.chain.from_iterable(gpes)))

    df["locs"] = df["GPE"].apply(lambda x: [e.text.lower() for e in x])
    return df.loc[:, ["username", "locs"]].groupby("username").agg(agg_gpe)


def check_locs(loc_df, city_df, denylist):
    """Filter out any hardcoded, undesired locations."""
    for c in ["city", "state", "county"]:
        loc_df[c] = loc_df["locs"].apply(
            lambda x: sorted(
                [(k, v) for k, v in x.items() if k in city_df[c].tolist() and k not in denylist],
                key=lambda x: x[1],
                reverse=True,
            )
        )
    return loc_df


def _predict(loc_df, cit_df):
    """Strategy for guessing a users location."""
    users = []
    guesses = []
    conf = []
    for u, cits, states in zip(
        loc_df["username"].tolist(), loc_df["filt_city"].tolist(), loc_df["state"].tolist()
    ):
        users.append(u)
        cur_cities = cits.copy()
        total_freqs = sum(c[1] for c in cur_cities)
        not_found = True
        while len(cur_cities) > 0 and not_found:
            c, f = cur_cities.pop(0)
            if len(set(states) & set(states)) > 0:
                guesses.append(c)
                conf.append(f / total_freqs)
                not_found = False
        if not_found:
            guesses.append(None)
            conf.append(None)
    return pd.DataFrame({"user": users, "guess": guesses, "conf": conf})


def predict(doc: nlp, )
