import csv
from pathlib import Path
from datetime import datetime
from threading import Lock
from typing import Optional
import discord
import pytz

from utils.logger import setup_logger

# Initialize logger
logger = setup_logger(__name__)

# Set timezone to Edmonton
LOCAL_TZ = pytz.timezone('America/Edmonton')

# CSV file path for flagged messages dataset
FLAGGED_MESSAGES_CSV = Path("data/flagged_messages_dataset.csv")
CSV_LOCK = Lock()  # Thread-safe file writing


class DatasetLogger:
    """Handles logging of flagged messages to CSV for training dataset."""
    
    def __init__(self):
        """Initialize the dataset logger."""
        self._initialize_csv()
    
    def _initialize_csv(self):
        """Initialize the CSV file with headers if it doesn't exist."""
        try:
            # Create data directory if it doesn't exist
            FLAGGED_MESSAGES_CSV.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"[CSV] Data directory ensured at: {FLAGGED_MESSAGES_CSV.parent}")
            
            # Check if file exists
            file_exists = FLAGGED_MESSAGES_CSV.exists()
            
            if file_exists:
                logger.info(f"[CSV] Found existing dataset file: {FLAGGED_MESSAGES_CSV}")
                # Verify file is readable
                with open(FLAGGED_MESSAGES_CSV, 'r', encoding='utf-8', newline='') as f:
                    reader = csv.reader(f)
                    row_count = sum(1 for row in reader)
                    logger.info(f"[CSV] Dataset contains {row_count} rows (including header)")
            else:
                logger.info(f"[CSV] Creating new dataset file: {FLAGGED_MESSAGES_CSV}")
                # Create file with headers
                with open(FLAGGED_MESSAGES_CSV, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                    headers = [
                        'timestamp',
                        'user_id',
                        'username',
                        'user_discriminator',
                        'guild_id',
                        'guild_name',
                        'channel_id',
                        'channel_name',
                        'message_content',
                        'confidence',
                        'detection_reason',
                        'user_joined_at',
                        'message_id'
                    ]
                    writer.writerow(headers)
                    logger.info(f"[CSV] Created dataset with headers: {headers}")
                    
        except Exception as e:
            logger.error(f"[CSV] Error initializing CSV file: {e}", exc_info=True)
    
    def log_flagged_message(
        self,
        message: discord.Message,
        confidence: float,
        reason: str,
        user_joined_at: str
    ):
        """
        Log a flagged message to the CSV dataset.
        
        Args:
            message: The Discord message that was flagged
            confidence: Detection confidence score (0.0 to 1.0)
            reason: Reason for detection (e.g., "ML Detection", "Pattern Detection")
            user_joined_at: When the user joined the server (formatted string)
        """
        try:
            logger.info(f"[CSV] Preparing to log message from {message.author.name} to dataset")
            
            # Prepare data row
            timestamp = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
            
            row = [
                timestamp,
                str(message.author.id),
                message.author.name,
                str(message.author.discriminator),
                str(message.guild.id) if message.guild else "DM",
                message.guild.name if message.guild else "DM",
                str(message.channel.id),
                message.channel.name if hasattr(message.channel, 'name') else "Unknown",
                message.content,  # CSV writer will handle escaping
                f"{confidence:.4f}",
                reason,
                user_joined_at,
                str(message.id)
            ]
            
            logger.debug(f"[CSV] Row data prepared: user={message.author.name}, confidence={confidence:.4f}")
            
            # Thread-safe write to CSV
            with CSV_LOCK:
                logger.debug(f"[CSV] Acquired file lock, writing to {FLAGGED_MESSAGES_CSV}")
                with open(FLAGGED_MESSAGES_CSV, 'a', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                    writer.writerow(row)
                    logger.info(f"[CSV] Successfully logged message to dataset")
                    
            # Log file size for monitoring
            file_size = FLAGGED_MESSAGES_CSV.stat().st_size
            logger.debug(f"[CSV] Current dataset file size: {file_size:,} bytes")
            
        except Exception as e:
            logger.error(f"[CSV] Error logging to dataset: {e}", exc_info=True)
    
    @staticmethod
    def get_dataset_stats() -> dict:
        """
        Get statistics about the dataset.
        
        Returns:
            Dictionary with dataset statistics
        """
        try:
            if not FLAGGED_MESSAGES_CSV.exists():
                return {
                    'exists': False,
                    'total_messages': 0,
                    'file_size': 0,
                    'detection_methods': {}
                }
            
            # Read dataset stats
            with open(FLAGGED_MESSAGES_CSV, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            total_messages = len(rows)
            file_size = FLAGGED_MESSAGES_CSV.stat().st_size
            
            # Get detection method breakdown
            detection_methods = {}
            for row in rows:
                method = row.get('detection_reason', 'Unknown')
                detection_methods[method] = detection_methods.get(method, 0) + 1
            
            return {
                'exists': True,
                'total_messages': total_messages,
                'file_size': file_size,
                'file_path': str(FLAGGED_MESSAGES_CSV),
                'detection_methods': detection_methods
            }
            
        except Exception as e:
            logger.error(f"[CSV] Error reading dataset stats: {e}", exc_info=True)
            return {
                'exists': False,
                'total_messages': 0,
                'file_size': 0,
                'detection_methods': {},
                'error': str(e)
            }