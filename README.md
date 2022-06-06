# Crypto Streaming

An attempt at real-time cryptocurrencies price prediction based on social media sentiments. We use Spark Streaming to process input data and to generate the sentiment scores.

This is a term project of CSCI-GA. 2437 (Big Data Application Development).

## Data Ingestion
### Data sources
  * Telegram

Consists of two parts:
  * Python using Telethon Library.
    * Package under `fetch_telegram/`.
    * ```usage: python3 fetch_telegram```
    * You'll need to insert your `api_id` and `api_hash` in the script.
  * Sentiment Analysis using Spark Streaming.
    * Package under `ReceiveStream/`.
    * usage:
    ```
    spark-submit \
        --packages com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.3 \
        --class ReceiveStream target/scala-2.12/receive-stream_2.12-0.0.1.jar \
        <HOST> <PORT>
    ```

## Analysis
A walk through is available in `lstm_notebook/LSTM.ipynb`.
