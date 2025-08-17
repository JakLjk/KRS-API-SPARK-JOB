from spark_processors.krs_api_processor.krs_api_loader_bronze import krs_api_job as bronze
from spark_processors.krs_api_processor.krs_api_loader_silver import krs_api_job as silver
from spark_processors.krs_api_processor.krs_api_loader_gold import krs_api_job as gold



if __name__ == "__main__":
    # bronze()
    silver()
    # gold()