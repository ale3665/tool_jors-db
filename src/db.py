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
        """
        Initializes a SQLAlchemy database engine and metadata object.

        This constructor creates a SQLAlchemy engine for interacting with a SQLite database.
        It uses the provided file path (fp) to create a database file.
        The metadata object is initialized to manage the database schema.

        Args:
            fp: A Path object representing the file path to the SQLite database.
        """  # noqa: E501
        self.engine: Engine = create_engine(url=f"sqlite:///{fp}")
        self.metadata: MetaData = MetaData()

    def createTables(self) -> None:
        """
        Creates the necessary database tables for storing JOSS paper metadata.

        This method defines and creates three tables: 'front_matter', 'metadata', and 'software'.
        The 'front_matter' table stores the raw HTML content of the papers.
        The 'metadata' table stores structured metadata extracted from the HTML.
        The 'software' table stores additional information about the software projects.

        It uses the SQLAlchemy engine and metadata objects to define the table schemas
        and then executes the `create_all()` method to create the tables in the database.
        """  # noqa: E501
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
        """
        Inserts a Pandas DataFrame into the database table.

        This method takes a Pandas DataFrame and a table name as input and inserts the
        data from the DataFrame into the specified table in the database. It uses the
        SQLAlchemy engine to interact with the database.

        Args:
            df: A Pandas DataFrame containing the data to be inserted.
            table: The name of the table to insert the data into.
        """  # noqa: E501
        df.to_sql(
            name=table,
            con=self.engine,
            if_exists="append",
            index=True,
            index_label="id",
        )
