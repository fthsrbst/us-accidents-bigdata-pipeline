"""
Kafka Producer for US Accidents Data
Streams CSV data to Kafka topic in batches
"""

from kafka import KafkaProducer
import pandas as pd
import json
import time
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducer")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_SERVERS', 'kafka:9092')
KAFKA_TOPIC = 'us_accidents'
BATCH_SIZE = 1000
DELAY_BETWEEN_BATCHES = 0.5  # seconds

def create_producer():
    """Create Kafka producer"""
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=100
    )
    
    logger.info("Kafka producer created successfully")
    return producer

def stream_csv_to_kafka(csv_path, producer, topic, max_records=None):
    """Stream CSV file to Kafka topic"""
    logger.info(f"Reading CSV file: {csv_path}")
    
    # Read CSV in chunks for memory efficiency
    chunk_size = BATCH_SIZE
    total_sent = 0
    
    try:
        for chunk_num, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
            logger.info(f"Processing chunk {chunk_num + 1} ({len(chunk)} records)")
            
            for _, row in chunk.iterrows():
                # Convert row to dict
                record = row.to_dict()
                
                # Handle NaN values
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                
                # Use ID as key
                key = record.get('ID', str(total_sent))
                
                # Send to Kafka
                producer.send(topic, key=key, value=record)
                total_sent += 1
                
                if max_records and total_sent >= max_records:
                    logger.info(f"Reached max records limit: {max_records}")
                    break
            
            # Ensure messages are sent
            producer.flush()
            
            logger.info(f"Sent {total_sent} records to Kafka topic '{topic}'")
            
            # Small delay between batches
            time.sleep(DELAY_BETWEEN_BATCHES)
            
            if max_records and total_sent >= max_records:
                break
        
        logger.info(f"Streaming complete. Total records sent: {total_sent}")
        return total_sent
        
    except Exception as e:
        logger.error(f"Error streaming to Kafka: {str(e)}")
        raise

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("KAFKA PRODUCER - US ACCIDENTS DATA")
    logger.info("=" * 60)
    
    csv_path = "/opt/spark-data/US_Accidents_March23.csv"
    
    # Check if file exists
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        return
    
    try:
        # Create producer
        producer = create_producer()
        
        # Stream data (limit to first 100K for testing)
        total = stream_csv_to_kafka(
            csv_path=csv_path,
            producer=producer,
            topic=KAFKA_TOPIC,
            max_records=100000  # Limit for testing
        )
        
        # Close producer
        producer.close()
        
        logger.info(f"Successfully streamed {total} records to Kafka")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
