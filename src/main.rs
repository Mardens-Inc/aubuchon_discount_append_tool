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
    let mut file = csv::Reader::from_path("file.csv")?;
    for result in file.deserialize::<CSVRow>() {
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

    // Use tokio's stream processing or a bounded concurrent task approach
    use futures::stream::{self, StreamExt};

    // Process in batches with controlled concurrency
    let concurrency_limit = 50; // Adjust based on your system capabilities

    let results = stream::iter(rows)
        .map(|row| {
            let pool = pool.clone();
            async move {
                if let Some(mardens_price) = row.mardens_price {
                    match sqlx::query(
                        r#"UPDATE `DQ8weMwxbW` SET mp = ?, discount_code = ? WHERE prodaltkey = ?"#,
                    )
                    .bind(mardens_price)
                    .bind(&row.discount)
                    .bind(&row.upc)
                    .execute(&pool)
                    .await
                    {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            error!("Error updating database for UPC {}: {}", row.upc, e);
                            Err(e)
                        }
                    }
                } else {
                    Ok(())
                }
            }
        })
        .buffer_unordered(concurrency_limit) // Process up to concurrency_limit tasks in parallel
        .collect::<Vec<_>>()
        .await;

    // Check for errors if needed
    for result in results {
        if let Err(e) = result {
            // Handle or log errors
            error!("Task error: {}", e);
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
