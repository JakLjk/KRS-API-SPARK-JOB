

def write_to_delta(
        df, 
        path:str, 
        overwrite_schema:bool):
    (
        df
        .write
        .format("delta")
        .option("overwriteSchema", overwrite_schema)
        .mode("overwrite")
        .save(path)
    )