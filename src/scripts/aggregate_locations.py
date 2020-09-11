import pandas as pd


def to_lower(maybe_string):
    return maybe_string.lower() if type(maybe_string) == str else None


def main():

    # collect data
    print("Reading data .....")
    city_state = pd.read_csv("data/locations/cities-states.csv", sep="|")
    neighborhood = pd.read_csv("data/locations/neighborhoods.csv")
    foreign = pd.read_csv("data/locations/international.csv")

    # transform data
    print("Transforming data .....")
    neighborhood = neighborhood \
        .loc[:,["RegionName", "City", "CountyName", "State"]] \
        .applymap(to_lower)

    neighborhood["CountyName"] = neighborhood["CountyName"].apply(lambda x: x.replace("county", "").strip())

    city_state = city_state \
        .loc[:, ["City", "County", "State short", "State full"]] \
        .applymap(to_lower)

    foreign = foreign \
        .loc[:, ["Country_or_Area"]] \
        .applymap(to_lower)

    state_map = {row["State short"]: row["State full"] for _, row in city_state.iterrows()}

    # group data
    locations = neighborhood.merge(
        city_state,
        how="right",
        left_on=["City", "CountyName", "State"],
        right_on=["City", "County", "State short"]
    )

    locations["country"] = ["united states of america"] * locations.shape[0]

    locations = locations.merge(
        foreign,
        how="outer",
        left_on=["country"],
        right_on=["Country_or_Area"]
    ).loc[:, ["RegionName", "City", "County", "State", "Country_or_Area"]].drop_duplicates()

    locations.columns = ["neighborhood", "city", "county", "state", "country"]

    locations["state_full"] = locations["state"].apply(lambda x: state_map[x] if type(x) == str else None)

    locations = locations.where(pd.notnull(locations), None)

    # write to disk
    filepath = "data/locations/grouped-locations.csv"
    locations.to_csv(filepath, index=None)
    print(f"Wrote grouped locations to {filepath} .....")


if __name__ == "__main__":
    main()
