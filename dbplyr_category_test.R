if (!require("pacman")) install.packages("pacman")
pacman::p_load(dplyr, dbplyr, duckdb)

# 1. In-memory DuckDB connection ####
con <- dbConnect(duckdb::duckdb())

# 2. File paths ####
sales_path    <- "C:/Users/abdul/Documents/RStudio Files/Data/large_sales_with_category_corrected.csv"
products_path <- "C:/Users/abdul/Documents/RStudio Files/Data/product_info_corrected.csv"

# 3. Create DuckDB tables from CSVs ####
dbExecute(con, sprintf("
  CREATE TABLE sales AS
  SELECT * FROM read_csv_auto('%s');", sales_path))

dbExecute(con, sprintf("
  CREATE TABLE products AS
  SELECT * FROM read_csv_auto('%s');", products_path))

# 4. Lazy table references ####
sales_tbl    <- tbl(con, "sales")
products_tbl <- tbl(con, "products")

# 5. Query one table ####
target_products <- products_tbl %>%
  filter(category == "Tech") %>%
  select(product_id)

# 6. Use that result to filter another table ####
final_query <- sales_tbl %>%
  semi_join(target_products, by = "product_id")

# 7. Reveal SQL ####
show_query(final_query) 

# 8. Execute ####
df_results <- final_query %>% collect()

# 9. Shutdown ####
dbDisconnect(con, shutdown = TRUE)
