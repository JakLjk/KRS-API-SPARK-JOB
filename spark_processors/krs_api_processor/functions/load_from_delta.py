
def load_delta(
        spark_engine, 
        url:str, 
        repartition_by:str="company_krs",
        sort_within_partition_by:list=["company_krs", "valid_from_entry"]):
    df = spark_engine.read.format("delta").load(url)
    df = df.repartition(24, repartition_by).sortWithinPartitions(*sort_within_partition_by)
    return df