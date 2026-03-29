# set default catalog and schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")


# Set the default catalog and schema pyspark
spark.catalog.setCurrentCatalog(catalog_name)
spark.catalog.setCurrentDatabase(schema_name)

# display available tables in your schema
spark.catalog.listTables(schema_name)