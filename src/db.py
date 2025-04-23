from pathlib import Path

from pandas import DataFrame
from sqlalchemy import (
    JSON,
    Column,
    Engine,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)


class DB:
    def __init__(self, fp: Path) -> None:
        self.engine: Engine = create_engine(url=f"sqlite:///{fp}")
        self.metadata: MetaData = MetaData()

    def createTables(self) -> None:
        _: Table = Table(
            "front_matter",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("url", String, nullable=False),
            Column("page", Integer, nullable=False),
            Column("status_code", Integer, nullable=False),
            Column("html", String, nullable=False),
        )

        _: Table = Table(
            "metadata",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column(
                "front_matter_id",
                Integer,
                ForeignKey("front_matter.id"),
                nullable=False,
            ),
            Column("doi", String, nullable=False),
            Column("status", String, nullable=False),
            Column("time", String, nullable=False),
            Column("title", String, nullable=False),
            Column("badges", JSON, nullable=None),
            Column("submitter", JSON, nullable=False),
        )

        _: Table = Table(
            "software",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column(
                "metadata_id",
                Integer,
                ForeignKey("metadata.id"),
                nullable=False,
            ),
            Column("status_code", Integer, nullable=False),
            Column("repository_url", String, nullable=False),
            Column("html", String, nullable=False),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def df2table(self, df: DataFrame, table: str) -> None:
        df.to_sql(
            name=table,
            con=self.engine,
            if_exists="append",
            index=True,
            index_label="id",
        )
