import duckdb
connection = duckdb.connect(":memory:")

outputParquetPath = 'D:/VS Code/FullStackDashboardsProject/BooksStoreFull/BookStoreEtl/data/transformed/2.Stage2/Books_Data.parquet'

connection.execute(f"""
                   CREATE TABLE Books_Data AS SELECT * FROM read_parquet('data/transformed/1.Stage1/Books.parquet')
                   """)
query = f"""SELECT * FROM Books_Data bd"""
connection.execute(
    f"""
    COPY ({query}) TO '{outputParquetPath}' (FORMAT PARQUET)
    """
)

print ("Books Stage 2 Done !! 💖")