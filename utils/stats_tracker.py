import psutil
import json
from pathlib import Path
from datetime import datetime, timedelta
import pytz

from utils.logger import setup_logger
from utils.dataset_logger import DatasetLogger

logger = setup_logger(__name__)

# Timezone
LOCAL_TZ = pytz.timezone('America/Edmonton')

# Persistent stats file (lightweight JSON)
STATS_FILE = Path("data/bot_stats.json")


class StatsTracker:
    """Track bot statistics with hybrid storage: persistent overall + session stats."""
    
    def __init__(self):
        """Initialize the stats tracker."""
        self.session_start_time = datetime.now(LOCAL_TZ)
        
        # Session stats (reset on restart)
        self.session_messages_analyzed = 0
        self.session_messages_flagged = 0
        
        # Overall stats (persistent across restarts)
        self.overall_stats = self._load_overall_stats()
        
        logger.info("[STATS] Stats tracker initialized")
        logger.info(f"[STATS] Overall: {self.overall_stats['total_messages_analyzed']} analyzed, "
                   f"{self.overall_stats['total_messages_flagged']} flagged, "
                   f"{self.overall_stats['total_false_alarms']} false alarms")
    
    def _load_overall_stats(self) -> dict:
        """Load overall stats from JSON file or create new."""
        try:
            if STATS_FILE.exists():
                with open(STATS_FILE, 'r', encoding='utf-8') as f:
                    stats = json.load(f)
                    logger.info(f"[STATS] Loaded overall stats from {STATS_FILE}")
                    return stats
            else:
                # Create new stats file
                STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
                default_stats = {
                    'total_messages_analyzed': 0,
                    'total_messages_flagged': 0,
                    'total_false_alarms': 0,
                    'first_started': datetime.now(LOCAL_TZ).isoformat(),
                    'last_updated': datetime.now(LOCAL_TZ).isoformat()
                }
                self._save_overall_stats(default_stats)
                logger.info(f"[STATS] Created new stats file at {STATS_FILE}")
                return default_stats
        except Exception as e:
            logger.error(f"[STATS] Error loading stats: {e}", exc_info=True)
            return {
                'total_messages_analyzed': 0,
                'total_messages_flagged': 0,
                'total_false_alarms': 0,
                'first_started': datetime.now(LOCAL_TZ).isoformat(),
                'last_updated': datetime.now(LOCAL_TZ).isoformat()
            }
    
    def _save_overall_stats(self, stats: dict = None):
        """Save overall stats to JSON file."""
        try:
            if stats is None:
                stats = self.overall_stats
            
            stats['last_updated'] = datetime.now(LOCAL_TZ).isoformat()
            
            with open(STATS_FILE, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2)
            
            logger.debug(f"[STATS] Saved overall stats to {STATS_FILE}")
        except Exception as e:
            logger.error(f"[STATS] Error saving stats: {e}", exc_info=True)
    
    def increment_analyzed(self):
        """Increment the count of messages analyzed (both session and overall)."""
        self.session_messages_analyzed += 1
        self.overall_stats['total_messages_analyzed'] += 1
        self._save_overall_stats()
    
    def increment_flagged(self):
        """Increment the count of messages flagged (both session and overall)."""
        self.session_messages_flagged += 1
        self.overall_stats['total_messages_flagged'] += 1
        self._save_overall_stats()
    
    def increment_false_alarm(self):
        """Increment the count of false alarms reported (both session and overall)."""
        self.overall_stats['total_false_alarms'] += 1
        self._save_overall_stats()
        logger.info(f"[STATS] False alarm reported. Total: {self.overall_stats['total_false_alarms']}")
    
    def get_session_uptime(self) -> str:
        """
        Get session uptime as a formatted string.
        
        Returns:
            Formatted uptime string (e.g., "2 hours, 45 minutes")
        """
        uptime = datetime.now(LOCAL_TZ) - self.session_start_time
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        parts = []
        if days > 0:
            parts.append(f"{days} day{'s' if days != 1 else ''}")
        if hours > 0:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        
        return ", ".join(parts) if parts else "Less than a minute"
    
    def get_total_uptime(self) -> str:
        """
        Get total uptime since first bot start.
        
        Returns:
            Formatted total uptime string
        """
        try:
            first_started = datetime.fromisoformat(self.overall_stats['first_started'])
            total_uptime = datetime.now(LOCAL_TZ) - first_started
            
            days = total_uptime.days
            hours, remainder = divmod(total_uptime.seconds, 3600)
            
            if days > 0:
                return f"{days} day{'s' if days != 1 else ''}, {hours} hour{'s' if hours != 1 else ''}"
            else:
                return f"{hours} hour{'s' if hours != 1 else ''}"
        except:
            return "Unknown"
    
    def get_session_messages_per_hour(self) -> float:
        """Calculate session messages analyzed per hour."""
        uptime = datetime.now(LOCAL_TZ) - self.session_start_time
        hours = uptime.total_seconds() / 3600
        
        if hours < 0.01:
            return 0.0
        
        return self.session_messages_analyzed / hours
    
    def get_session_detection_rate(self) -> float:
        """Calculate session detection rate (flagged/analyzed)."""
        if self.session_messages_analyzed == 0:
            return 0.0
        
        return (self.session_messages_flagged / self.session_messages_analyzed) * 100
    
    def get_overall_detection_rate(self) -> float:
        """Calculate overall detection rate (flagged/analyzed)."""
        total = self.overall_stats['total_messages_analyzed']
        if total == 0:
            return 0.0
        
        return (self.overall_stats['total_messages_flagged'] / total) * 100
    
    def get_overall_accuracy_estimate(self) -> float:
        """Estimate overall accuracy based on false alarms reported."""
        flagged = self.overall_stats['total_messages_flagged']
        if flagged == 0:
            return 100.0
        
        false_alarms = self.overall_stats['total_false_alarms']
        true_positives = flagged - false_alarms
        
        if true_positives < 0:
            true_positives = 0
        
        return (true_positives / flagged) * 100
    
    @staticmethod
    def get_system_stats() -> dict:
        """Get system resource usage statistics."""
        try:
            process = psutil.Process()
            
            cpu_percent = process.cpu_percent(interval=0.1)
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            
            system_cpu = psutil.cpu_percent(interval=0.1)
            system_memory = psutil.virtual_memory()
            disk = psutil.disk_usage('.')
            
            return {
                'process_cpu_percent': cpu_percent,
                'process_memory_mb': memory_mb,
                'system_cpu_percent': system_cpu,
                'system_memory_percent': system_memory.percent,
                'system_memory_total_gb': system_memory.total / 1024 / 1024 / 1024,
                'system_memory_used_gb': system_memory.used / 1024 / 1024 / 1024,
                'disk_total_gb': disk.total / 1024 / 1024 / 1024,
                'disk_used_gb': disk.used / 1024 / 1024 / 1024,
                'disk_percent': disk.percent
            }
        except Exception as e:
            logger.error(f"[STATS] Error getting system stats: {e}", exc_info=True)
            return {}
    
    def get_comprehensive_stats(self) -> dict:
        """Get all statistics in one dictionary."""
        dataset_stats = DatasetLogger.get_dataset_stats()
        system_stats = self.get_system_stats()
        
        return {
            # Session stats
            'session_uptime': self.get_session_uptime(),
            'session_messages_analyzed': self.session_messages_analyzed,
            'session_messages_flagged': self.session_messages_flagged,
            'session_detection_rate': self.get_session_detection_rate(),
            'session_messages_per_hour': self.get_session_messages_per_hour(),
            
            # Overall stats
            'total_uptime': self.get_total_uptime(),
            'total_messages_analyzed': self.overall_stats['total_messages_analyzed'],
            'total_messages_flagged': self.overall_stats['total_messages_flagged'],
            'total_false_alarms': self.overall_stats['total_false_alarms'],
            'total_detection_rate': self.get_overall_detection_rate(),
            'overall_accuracy': self.get_overall_accuracy_estimate(),
            
            # Dataset stats
            'dataset_total': dataset_stats.get('total_messages', 0),
            'dataset_size_bytes': dataset_stats.get('file_size', 0),
            'dataset_size_mb': dataset_stats.get('file_size', 0) / 1024 / 1024,
            'detection_methods': dataset_stats.get('detection_methods', {}),
            
            # System stats
            'system': system_stats
        }