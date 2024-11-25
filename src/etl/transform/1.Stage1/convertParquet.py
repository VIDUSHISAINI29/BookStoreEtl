import duckdb
connection = duckdb.connect(":memory:")

csvFilesArray = ['data/raw/books.csv', 'data/raw/user_reviews_dataset.csv']

tableNamesArray = ['Books', 'BooksReviews']

outputParquetFilesPathArray = ['D:/VS Code/FullStackDashboardsProject/BooksStoreFull/BookStoreEtl/data/transformed/1.Stage1/Books.parquet', 'D:/VS Code/FullStackDashboardsProject/BooksStoreFull/BookStoreEtl/data/transformed/1.Stage1/BooksReview.parquet']

for csvFile, tableName, outputParquetFilesPath in zip(csvFilesArray, tableNamesArray, outputParquetFilesPathArray):
    connection.execute(
        f"""
        CREATE TABLE {tableName} AS SELECT * FROM read_csv_auto('{csvFile}')
        """
    )
    query = f"""SELECT * FROM {tableName}"""
    connection.execute(
        f"""
        COPY ({query}) TO '{outputParquetFilesPath}'
        (FORMAT PARQUET)
        """
    )
    
    print ("Books Stage 1 Done !! ✨")