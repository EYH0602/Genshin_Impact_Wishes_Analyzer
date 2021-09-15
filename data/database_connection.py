
from sqlalchemy import create_engine, desc
import pandas as pd
import json
from .mapped_classes import \
    CharacterWishes, NoviceWishes, WeaponWishes, StandardWishes
from sqlalchemy.orm import sessionmaker

model_map = {
    "character_wishes": CharacterWishes,
    "novice_wishes": NoviceWishes,
    "weapon_wishes": WeaponWishes,
    "standard_wishes": StandardWishes
}


class DataBase():
    def __init__(self):
        with open("data/config.json") as f:
            config = json.load(f)
        self.username = config['username']
        self.password = config['password']
        self.host = config['host']
        url = "mysql+pymysql://{}:{}@{}/genshine_impact_wishes".format(
            self.username, self.password, self.host
        )
        self.engine = create_engine(url)
        self.Session = sessionmaker(bind=self.engine)

    def __get_max_timestamp(self, table: str):
        session = self.Session()
        model = model_map[table]
        latest_time = session.query(model.time) \
            .order_by(desc(model.id)) \
            .first()
        session.close()
        return latest_time if latest_time is None else latest_time[0]

    def __get_min_timestamp(self, table: str):
        session = self.Session()
        model = model_map[table]
        first_time = session.query(model.time) \
            .order_by(model.id) \
            .first()
        session.close()
        return first_time if first_time is None else first_time[0]

    def append(self, table: str, source: str = "df"):
        if source not in ["df", "csv"]:
            raise RuntimeError("Unknown data source.")

        def append_from_dataframe(df: pd.DataFrame):
            lastest_time = self.__get_max_timestamp(table=table)
            # to make sure no extra rows are appended
            if lastest_time is not None:
                df = df[df.time > lastest_time.strftime("%Y-%m-%d %H:%M:%S")]
            df.to_sql(table, con=self.engine, if_exists='append', index=False)

        def append_from_csv(file: str):
            df = pd.read_csv(file)
            append_from_dataframe(df)

        return append_from_dataframe if source == "df" else append_from_csv

    def get_table(self, table: str):
        df = pd.read_sql_table(table, self.engine)
        df["time"] = df["time"].astype(str)
        return df.drop(columns=["id"])

    def get_time_range(self, table: str):
        return self.__get_min_timestamp(table), self.__get_max_timestamp(table)

    def get_total_count(self, table: str) -> int:
        session = self.Session()
        model = model_map[table]
        id_max = session.query(model.id) \
            .order_by(desc(model.id)) \
            .first()
        session.close()
        return id_max[0]

    def get_wishes_count(self, table: str, rank_type: int) -> int:
        session = self.Session()
        model = model_map[table]
        count = session.query(model.id) \
            .filter(model.rank_type == rank_type) \
            .count()
        session.close()
        return count[0]

    def get_last_wish(self, table: str, rank_type: int):
        session = self.Session()
        model = model_map[table]
        wish = session.query(model.id) \
            .filter(model.rank_type == rank_type) \
            .order_by(desc(model.id)) \
            .first()
        session.close()
        return wish

    def get_wishes(self, table: str, rank_type: int):
        """
        get a list of wishes from the given table
        """
        session = self.Session()
        model = model_map[table]
        wishes = session.query(model.id, model.name) \
            .filter(model.rank_type == rank_type) \
            .all()
        session.close()
        return wishes
