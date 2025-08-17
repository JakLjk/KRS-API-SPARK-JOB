from pyspark.sql import SparkSession, functions as F, types as T, Window

from .schemas.schema_raw_krs_api_json import schema as krs_api_schema
from .functions.write_to_delta import write_to_delta

from config import(
    ENV_APP_NAME,
    ENV_MASTER_URL,
    ENV_HADOOP_USER,
    ENV_TIMEZONE,
    ENV_ALLOW_SCHEMA_OVERWRITE,

    ENV_REPARTITION_SIZE,

    ENV_DELTA_BRONZE_RAW_DATA,

    ENV_DELTA_SILVER_REGISTRY,
    ENV_DELTA_SILVER_HISTORY_NAMES,
    ENV_DELTA_SILVER_HISTORY_IDENTIFIERS,
    ENV_DELTA_SILVER_HISTORY_LEGAL_FORM,
    ENV_DELTA_SILVER_HISTORY_HAS_OPP_STATUS,
    ENV_DELTA_SILVER_HISTORY_BUSINESS_WITH_OTHERS,
    ENV_DELTA_SILVER_HISTORY_ADDRESS,
    ENV_DELTA_SILVER_HISTORY_HEADQUARTERS,
    ENV_DELTA_SILVER_HISTORY_WEBPAGE,
    ENV_DELTA_SILVER_HISTORY_EMAIL,
    ENV_DELTA_SILVER_HISTORY_ELECTRONIC_BAE_DELIVERY,
    ENV_DELTA_SILVER_HISTORY_MAIN_HQ_ADDRESS,
    ENV_DELTA_SILVER_HISTORY_TIME_ENTITY_CREATED,
    ENV_DELTA_SILVER_HISTORY_STATUTE_CHANGE,
    ENV_DELTA_SILVER_HISTORY_SHARE_CAPITAL_AMOUNT,
    ENV_DELTA_SILVER_HISTORY_TOTAL_SHARES_UNITS,
    ENV_DELTA_SILVER_HISTORY_SINGLE_SHARE_VALUE,
    ENV_DELTA_SILVER_HISTORY_PAID_IN_CAPITAL_PART,
    ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_SERIES_NAME,
    ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_SHARES_IN_SERIES,
    ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_PREFERENCE_INFO,
)



def krs_api_job():

    # Spark Session definition
    # Adds engines for handling postgresql and delta lake
    # Enabling Delta Lake extension to use delta-specific parser commands
    # Enabling spark-catalog to understand delta tablse 
    spark = (SparkSession.builder
        .appName(f"{ENV_APP_NAME}-SILVER")
        .master(ENV_MASTER_URL)
        .config("spark.jars.packages","io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.7.3")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", ENV_TIMEZONE)
        .config("spark.executorEnv.HADOOP_USER_NAME", ENV_HADOOP_USER)
        .config("spark.driverEnv.HADOOP_USER_NAME",  ENV_HADOOP_USER)
        .getOrCreate())

    # Load bronze layer from Delta Lake
    bronze_df = spark.read.format("delta").load(ENV_DELTA_BRONZE_RAW_DATA)
    
    # Checks if raw string is is formatted for 'danePodmiotuZagranicznego' if so, then function
    # will change strings to the standard format
    # Function adds schema to the raw json
    df = (
        bronze_df
        .withColumn("czy_firma_zagraniczna", F.when(F.instr(F.col("raw_json"), "danePodmiotuZagranicznego")>0, True).otherwise(False))
        .withColumn("raw_json", F.regexp_replace(
            F.col("raw_json"),
            r'"siedzibaIAdresPodmiotuZagranicznego"\s*:',
            r'"siedzibaIAdres":'
        ))
        .withColumn("raw_json", F.regexp_replace(
            F.col("raw_json"), 
            r'"([a-zA-Z]+)Zagranicznego"\s*:',
            r'"$1":')) 
        .withColumn("data", F.from_json("raw_json", krs_api_schema))
        .drop("raw_json")
    )
    tables = {}

    # Exploding nested records that have information about registry
    tables["df_registry"] = {
        "url": ENV_DELTA_SILVER_REGISTRY,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.naglowekP.wpis").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.opis").alias("description"),
            F.to_date(F.col("entry.dataWpisu"), "dd.MM.yyyy").alias("entry_date"),
            F.col("entry.numerWpisu").cast("int").alias("entry_number"),
            F.col("entry.oznaczenieSaduDokonujacegoWpisu").alias("court_designation"),
            F.col("entry.sygnaturaAktSprawyDotyczacejWpisu").alias("case_file_designation")
        )
    }

    # Exploding nested records that have information about comany names
    tables["df_hist_names"] = {
        "url": ENV_DELTA_SILVER_HISTORY_NAMES,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.danePodmiotu.nazwa").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.nazwa").alias("company_name"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }

    tables["df_hist_identifiers"] = {
        "url": ENV_DELTA_SILVER_HISTORY_IDENTIFIERS,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.danePodmiotu.identyfikatory").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.identyfikatory.nip").alias("nip_number"),
            F.col("entry.identyfikatory.regon").alias("regon_number"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }

    tables["df_hist_legal_form"] = {
        "url": ENV_DELTA_SILVER_HISTORY_LEGAL_FORM,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.danePodmiotu.formaPrawna").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.formaPrawna").alias("legal_form"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }

    tables["df_hist_has_opp_status"] = {
        "url": ENV_DELTA_SILVER_HISTORY_HAS_OPP_STATUS,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.danePodmiotu.czyPosiadaStatusOPP").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.czyPosiadaStatusOPP").alias("has_opp_status"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }

    tables["df_hist_does_conduct_business_with_other_entities"] = {
        "url": ENV_DELTA_SILVER_HISTORY_BUSINESS_WITH_OTHERS,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.danePodmiotu.czyProwadziDzialalnoscZInnymiPodmiotami").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.czyProwadziDzialalnoscZInnymiPodmiotami").alias("does_conduct_business_with_other_entities"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }


    tables["df_hist_address"] = {
        "url": ENV_DELTA_SILVER_HISTORY_ADDRESS,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.siedzibaIAdres.adres").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.kraj").alias("country"),
            F.col("entry.ulica").alias("street"),
            F.col("entry.nrDomu").alias("house_number"),
            F.col("entry.nrLokalu").alias("flat_number"),
            F.col("entry.poczta").alias("post_office"),
            F.col("entry.kodPocztowy").alias("postal_code"),
            F.col("entry.miejscowosc").alias("city"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }

    tables["df_hist_headquarters"] = {
        "url": ENV_DELTA_SILVER_HISTORY_HEADQUARTERS,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.siedzibaIAdres.siedziba").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.kraj").alias("country"),
            F.col("entry.gmina").alias("municipality"),
            F.col("entry.powiat").alias("county"),
            F.col("entry.miejscowosc").alias("city"),
            F.col("entry.wojewodztwo").alias("voivodeship"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }

    tables["df_hist_webpage"] = {
        "url": ENV_DELTA_SILVER_HISTORY_WEBPAGE,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.siedzibaIAdres.adresStronyInternetowej").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.adresStronyInternetowej").alias("webpage"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }

    tables["df_hist_email"] = {
        "url": ENV_DELTA_SILVER_HISTORY_EMAIL,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.siedzibaIAdres.adresPocztyElektronicznej").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.adresPocztyElektronicznej").alias("email"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }

    tables["df_hist_electronic_delivery_addres_base"] = {
        "url": ENV_DELTA_SILVER_HISTORY_ELECTRONIC_BAE_DELIVERY,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.siedzibaIAdres.adresDoDoreczenElektronicznychWpisanyDoBAE").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.adresDoDoreczenElektronicznychWpisanyDoBAE").alias("electronic_bae_delivery"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }

    tables["df_hist_main_headquarters_address"] = {
        "url": ENV_DELTA_SILVER_HISTORY_MAIN_HQ_ADDRESS,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.siedzibaIAdres.siedzibaIAdresZakladuGlownego").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.kraj").alias("country"),
            F.col("entry.ulica").alias("street"),
            F.col("entry.nrDomu").alias("house_number"),
            F.col("entry.nrLokalu").alias("flat_number"),
            F.col("entry.poczta").alias("post_office"),
            F.col("entry.kodPocztowy").alias("postal_code"),
            F.col("entry.miejscowosc").alias("city"),
            F.col("entry.wojewodztwo").alias("voivodeship"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }
    tables["df_hist_time_for_which_the_entity_was_created"] = {
        "url": ENV_DELTA_SILVER_HISTORY_TIME_ENTITY_CREATED,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.pozostaleInformacje.informacjaOCzasieNaJakiZostalUtworzonyPodmiot").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.czasNaJakiUtworzonyZostalPodmiot").alias("time_for_which_entity_was_created"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }

    tables["df_hist_conclusion_or_amendment_of_the_statue_agreement"] = {
        "url": ENV_DELTA_SILVER_HISTORY_STATUTE_CHANGE,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.umowaStatut.informacjaOZawarciuZmianieUmowyStatutu").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("entry.pozycja").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.zawarcieZmianaUmowyStatutu").alias("change_or_amendment_of_statue_agreement"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }
    tables["df_hist_share_capital_amount"] = {
        "url": ENV_DELTA_SILVER_HISTORY_SHARE_CAPITAL_AMOUNT,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode("data.odpis.dane.dzial1.kapital.wysokoscKapitaluZakladowego").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.waluta").alias("currency"),
            F.col("entry.wartosc").alias("share_capital_amount"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }
    tables["df_hist_total_number_of_shares_units"] = {
        "url": ENV_DELTA_SILVER_HISTORY_TOTAL_SHARES_UNITS,
        "df": df.select(
                F.col("source_id"),
                F.col("company_krs"),
                F.explode_outer("data.odpis.dane.dzial1.kapital.lacznaLiczbaAkcjiUdzialow").alias("entry")
            ).select(
                F.col("source_id"),
                F.col("company_krs"),
                F.col("entry.lacznaLiczbaAkcjiUdzialow").alias("total_number_of_shares_units"),
                F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
                F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
            )
    }
    tables["df_hist_single_share_value"] = {
        "url": ENV_DELTA_SILVER_HISTORY_SINGLE_SHARE_VALUE,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode_outer("data.odpis.dane.dzial1.kapital.wartoscJednejAkcji").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.waluta").alias("currency"),
            F.col("entry.wartosc").alias("single_share_value"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }
    tables["df_hist_paid_in_capital_part"] = {
        "url": ENV_DELTA_SILVER_HISTORY_PAID_IN_CAPITAL_PART,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode_outer("data.odpis.dane.dzial1.kapital.czescKapitaluWplaconegoPokrytego").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.waluta").alias("currency"),
            F.col("entry.wartosc").alias("paid_in_capital_amount"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.col("entry.nrWpisuWykr").cast("int").alias("valid_to_entry")
        )
    }
    tables["df_hist_share_issues_series_name"] = {
        "url": ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_SERIES_NAME,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode_outer("data.odpis.dane.dzial1.emisjeAkcji").alias("issue")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode_outer("issue.nazwaSeriiAkcji").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.nazwaSeriiAkcji").alias("series_name"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.lit(None).cast("int").alias("valid_to_entry")
        )
    }
    tables["df_hist_share_issues_shares_in_series"] = {
        "url": ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_SHARES_IN_SERIES,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode_outer("data.odpis.dane.dzial1.emisjeAkcji").alias("issue")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode_outer("issue.liczbaAkcjiWSerii").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.liczbaAkcjiWSerii").alias("shares_in_series"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.lit(None).cast("int").alias("valid_to_entry")
        )
    }
    tables["df_hist_share_issues_preference_info"] = {
        "url": ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_PREFERENCE_INFO,
        "df": df.select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode_outer("data.odpis.dane.dzial1.emisjeAkcji").alias("issue")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.explode_outer("issue.czyAkcjeUprzywilejowaneLiczbaAkcjiRodzajUprzywilejowania").alias("entry")
        ).select(
            F.col("source_id"),
            F.col("company_krs"),
            F.col("entry.czyAkcjeUprzywilejowaneLiczbaAkcjiRodzajUprzywilejowania").alias("preference_info"),
            F.col("entry.nrWpisuWprow").cast("int").alias("valid_from_entry"),
            F.lit(None).cast("int").alias("valid_to_entry")
        )
    }

    for name,data in tables.items():
        if name != "df_registry":
            data["df"] = (
                data["df"]
                .repartition(ENV_REPARTITION_SIZE, "company_krs")
                .sortWithinPartitions("valid_from_entry")
            )
        elif name == "df_registry":
            data["df"] = (
                data["df"]
                .repartition(ENV_REPARTITION_SIZE, "company_krs")
                .sortWithinPartitions("entry_number")
            )

    for name, data in tables.items():
        path = data["url"]
        out_df = data["df"]
        print(f"Saving table {name}")
        write_to_delta(out_df, path, ENV_ALLOW_SCHEMA_OVERWRITE)
        print(f"Saved table {name}")

