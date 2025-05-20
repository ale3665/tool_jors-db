from pathlib import Path
from pandas import DataFrame
from sqlalchemy import (
    JSON,
    Column,
    Engine,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)

class DB:
    def __init__(self, fp: Path) -> None:
        """
        Initializes a SQLAlchemy database engine and metadata object.

        This constructor creates a SQLAlchemy engine for interacting with a SQLite database.
        It uses the provided file path (fp) to create a database file.
        The metadata object is initialized to manage the database schema.

        Args:
            fp: A Path object representing the file path to the SQLite database.
        """
        self.engine: Engine = create_engine(url=f"sqlite:///{fp}")
        self.metadata: MetaData = MetaData()

    def create_tables(self) -> None:
        """
        Creates the "articles" table for storing JORS paper metadata.

        This method defines a single table named 'articles' which includes fields such as:
        title, authors, DOI, keywords, abstract, software availability, and raw HTML content.

        The table schema is defined using SQLAlchemy and then created in the database.
        """
        Table(
            "articles",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("url", String, nullable=False),
            Column("title", String, nullable=False),
            Column("authors", JSON, nullable=False),
            Column("doi", String, nullable=False),
            Column("keywords", JSON, nullable=True),
            Column("abstract", String, nullable=False),
            Column("software_availability", String, nullable=True),
            Column("raw_html", String, nullable=False),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def df2table(self, df: DataFrame, table: str) -> None:
        """
        Inserts a Pandas DataFrame into the specified database table.

        This method takes a DataFrame and appends its rows into the given table name
        within the database using the SQLAlchemy engine.

        Args:
            df: The Pandas DataFrame to insert.
            table: The name of the database table to insert into.
        """
        df.to_sql(
            name=table,
            con=self.engine,
            if_exists="append",
            index=False
        )

