use anyhow::Result;
use database_common_lib::database_connection::{create_pool, DatabaseConnectionData};
use log::*;
use serde::Deserialize;

#[derive(Debug)]
struct CSVRow {
    upc: String,
    retail: f32,
    discount: String,
    mardens_price: Option<f32>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::builder()
        .format_timestamp(None)
        .filter_level(LevelFilter::Debug)
        .init();
    info!("Starting...");
    let start_time = chrono::Local::now();
    let mut csv_values = load_csv_values()?;
    debug!("Loaded {} rows.", csv_values.len());
    debug!("{:?}", csv_values[0]);
    calculate_mardens_prices(&mut csv_values);
    debug!("Finished calculating mardens prices.");
    upload_to_database(csv_values).await?;

    info!(
        "Finished in {:?}",
        start_time.signed_duration_since(chrono::Local::now())
    );
    Ok(())
}

fn load_csv_values() -> Result<Vec<CSVRow>> {
    let mut results = vec![];
    let vartext = include_str!("../file.csv");
    let mut rdr = csv::ReaderBuilder::new()
    .has_headers(true)
    .from_reader(vartext.as_bytes());
    for result in rdr.deserialize::<CSVRow>() {
        let result = result?;
        if !result.discount.is_empty() {
            results.push(result);
        }
    }
    Ok(results)
}

fn calculate_mardens_prices(rows: &mut [CSVRow]) {
    for row in rows.iter_mut() {
        let code = row.discount.as_str();
        let percentage: f32 = match code {
            "a" => 0.4,
            "b" => 0.45,
            "c" => 0.5,
            "d" => 0.6,
            _ => 0.4,
        };
        row.mardens_price = Some(row.retail * (1f32 - percentage));
    }
}

async fn upload_to_database(rows: Vec<CSVRow>) -> Result<()> {
    let config = DatabaseConnectionData::get().await?;
    let pool = create_pool(&config).await?;

    // Set batch size for grouping queries
    let batch_size = 500; // Adjust based on your database capabilities and data size

    // Process rows in batches
    for chunk in rows.chunks(batch_size) {
        // Build a batch query with parameters for all rows in this chunk
        let mut query_builder = sqlx::QueryBuilder::new(
            "UPDATE `DQ8weMwxbW` SET mp = CASE prodaltkey "
        );

        // Add WHEN/THEN clauses for each row with mardens_price
        let mut params = Vec::new();
        let mut discount_updates = Vec::new();
        let mut has_valid_rows = false;

        for row in chunk {
            if let Some(mardens_price) = row.mardens_price {
                has_valid_rows = true;
                query_builder.push(" WHEN ");
                query_builder.push_bind(&row.upc);
                query_builder.push(" THEN ");
                query_builder.push_bind(mardens_price);

                params.push(&row.upc);
                discount_updates.push((row.upc.clone(), row.discount.clone()));
            }
        }

        // Skip if no valid rows in this batch
        if !has_valid_rows {
            continue;
        }

        // Complete the price update
        query_builder.push(" END, discount_code = CASE prodaltkey ");

        // Add WHEN/THEN clauses for discount codes
        for (upc, discount) in &discount_updates {
            query_builder.push(" WHEN ");
            query_builder.push_bind(upc);
            query_builder.push(" THEN ");
            query_builder.push_bind(discount);
        }

        // Finalize query with WHERE clause for all relevant UPCs
        query_builder.push(" END WHERE prodaltkey IN (");
        let mut separated = query_builder.separated(", ");
        for upc in params {
            separated.push_bind(upc);
        }
        separated.push_unseparated(")");

        // Execute the batch query
        match query_builder.build().execute(&pool).await {
            Ok(result) => {
                info!("Batch update succeeded: {} rows affected", result.rows_affected());
            },
            Err(e) => {
                error!("Error with batch update: {}", e);
                return Err(anyhow::anyhow!("Database batch update failed: {}", e));
            }
        }
    }

    Ok(())
}

impl<'de> Deserialize<'de> for CSVRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize, Debug)]
        struct RowHelper {
            #[serde(rename = "PRODALTKEY")]
            upc: String,
            #[serde(rename = " price1 ")]
            retail: String,
            #[serde(rename = "discount_code")]
            discount: String,
        }

        let helper = RowHelper::deserialize(deserializer).map_err(|e| {
            error!("Failed to deserialize row: {}", e);
            e
        })?;

        let retail = helper
            .retail
            .chars()
            .filter(|c| c.is_ascii_digit() || *c == '.')
            .collect::<String>()
            .parse::<f32>()
            .unwrap_or(0.0);

        Ok(CSVRow {
            upc: helper.upc,
            retail,
            discount: helper.discount,
            mardens_price: None,
        })
    }
}
