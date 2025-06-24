#!/usr/bin/env python3
"""
BAR Leaderboard Performance Monitoring
======================================

Performance monitoring and profiling utilities for all pipeline operations.
Tracks timing, memory usage, and system resource utilization.
"""

import time
import psutil
import threading
import sys
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from pathlib import Path
import json
from contextlib import contextmanager
from dataclasses import dataclass, asdict
import functools

from config import config
from utils import setup_logging

@dataclass
class PerformanceMetrics:
    """Container for performance metrics."""
    operation_name: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    memory_start_mb: Optional[float] = None
    memory_end_mb: Optional[float] = None
    memory_peak_mb: Optional[float] = None
    cpu_percent: Optional[float] = None
    records_processed: Optional[int] = None
    records_per_second: Optional[float] = None
    success: bool = True
    error_message: Optional[str] = None

class PerformanceMonitor:
    """Comprehensive performance monitoring for pipeline operations."""
    
    def __init__(self):
        self.logger = setup_logging(self.__class__.__name__)
        self.metrics: List[PerformanceMetrics] = []
        self.monitoring_active = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.system_metrics: List[Dict[str, Any]] = []
    
    @contextmanager
    def monitor_operation(self, operation_name: str, records_count: Optional[int] = None):
        """Context manager for monitoring a single operation."""
        metrics = PerformanceMetrics(
            operation_name=operation_name,
            start_time=time.time(),
            memory_start_mb=self._get_memory_usage(),
            records_processed=records_count
        )
        
        self.logger.info(f"🚀 Starting: {operation_name}")
        
        try:
            # Start system monitoring
            self._start_system_monitoring()
            
            yield metrics
            
            # Operation completed successfully
            metrics.success = True
            
        except Exception as e:
            # Operation failed
            metrics.success = False
            metrics.error_message = str(e)
            self.logger.error(f"💥 {operation_name} failed: {e}")
            raise
        
        finally:
            # Stop monitoring and calculate final metrics
            self._stop_system_monitoring()
            
            metrics.end_time = time.time()
            metrics.duration = metrics.end_time - metrics.start_time
            metrics.memory_end_mb = self._get_memory_usage()
            
            if self.system_metrics:
                metrics.memory_peak_mb = max(m['memory_mb'] for m in self.system_metrics)
                metrics.cpu_percent = sum(m['cpu_percent'] for m in self.system_metrics) / len(self.system_metrics)
            
            if metrics.records_processed and metrics.duration:
                metrics.records_per_second = metrics.records_processed / metrics.duration
            
            self.metrics.append(metrics)
            self._log_operation_summary(metrics)
    
    def monitor_function(self, operation_name: Optional[str] = None):
        """Decorator for monitoring function performance."""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                name = operation_name or f"{func.__module__}.{func.__name__}"
                with self.monitor_operation(name):
                    return func(*args, **kwargs)
            return wrapper
        return decorator
    
    def _start_system_monitoring(self) -> None:
        """Start background system monitoring."""
        if self.monitoring_active:
            return
        
        self.monitoring_active = True
        self.system_metrics = []
        
        def monitor_system():
            while self.monitoring_active:
                try:
                    self.system_metrics.append({
                        'timestamp': time.time(),
                        'memory_mb': self._get_memory_usage(),
                        'cpu_percent': psutil.cpu_percent(),
                        'disk_io': psutil.disk_io_counters()._asdict() if psutil.disk_io_counters() else {}
                    })
                    time.sleep(1)  # Sample every second
                except Exception as e:
                    self.logger.warning(f"System monitoring error: {e}")
                    break
        
        self.monitor_thread = threading.Thread(target=monitor_system, daemon=True)
        self.monitor_thread.start()
    
    def _stop_system_monitoring(self) -> None:
        """Stop background system monitoring."""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024  # Convert to MB
        except Exception:
            return 0.0
    
    def _log_operation_summary(self, metrics: PerformanceMetrics) -> None:
        """Log a summary of operation performance."""
        status = "✅" if metrics.success else "❌"
        duration_str = f"{metrics.duration:.2f}s" if metrics.duration else "N/A"
        
        self.logger.info(f"{status} {metrics.operation_name}: {duration_str}")
        
        if metrics.records_processed:
            rate_str = f"{metrics.records_per_second:,.0f} records/sec" if metrics.records_per_second else "N/A"
            self.logger.info(f"   📊 Processed: {metrics.records_processed:,} records ({rate_str})")
        
        if metrics.memory_start_mb and metrics.memory_end_mb:
            memory_delta = metrics.memory_end_mb - metrics.memory_start_mb
            peak_str = f", peak: {metrics.memory_peak_mb:.1f}MB" if metrics.memory_peak_mb else ""
            self.logger.info(f"   🧠 Memory: {metrics.memory_start_mb:.1f}MB → {metrics.memory_end_mb:.1f}MB (Δ{memory_delta:+.1f}MB{peak_str})")
        
        if metrics.cpu_percent:
            self.logger.info(f"   🖥️  CPU: {metrics.cpu_percent:.1f}% average")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get a comprehensive performance summary."""
        if not self.metrics:
            return {"message": "No performance data available"}
        
        total_duration = sum(m.duration for m in self.metrics if m.duration)
        successful_operations = [m for m in self.metrics if m.success]
        failed_operations = [m for m in self.metrics if not m.success]
        
        return {
            "total_operations": len(self.metrics),
            "successful_operations": len(successful_operations),
            "failed_operations": len(failed_operations),
            "total_duration_seconds": total_duration,
            "average_duration_seconds": total_duration / len(self.metrics) if self.metrics else 0,
            "total_records_processed": sum(m.records_processed for m in self.metrics if m.records_processed),
            "overall_records_per_second": sum(m.records_per_second for m in self.metrics if m.records_per_second),
            "memory_usage": {
                "peak_mb": max(m.memory_peak_mb for m in self.metrics if m.memory_peak_mb) if any(m.memory_peak_mb for m in self.metrics) else None,
                "total_allocated_mb": sum(max(0, (m.memory_end_mb or 0) - (m.memory_start_mb or 0)) for m in self.metrics)
            },
            "operations": [asdict(m) for m in self.metrics]
        }
    
    def save_performance_report(self, output_path: Optional[Path] = None) -> Path:
        """Save detailed performance report to JSON file."""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = config.paths.data_dir / f"performance_report_{timestamp}.json"
        
        report = {
            "generated_at": datetime.now().isoformat(),
            "system_info": {
                "cpu_count": psutil.cpu_count(),
                "memory_total_gb": psutil.virtual_memory().total / 1024**3,
                "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
            },
            "performance_summary": self.get_performance_summary()
        }
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        self.logger.info(f"📊 Performance report saved to: {output_path}")
        return output_path
    
    def print_performance_summary(self) -> None:
        """Print a formatted performance summary."""
        summary = self.get_performance_summary()
        
        if "message" in summary:
            self.logger.info(summary["message"])
            return
        
        self.logger.info("\n" + "="*60)
        self.logger.info("📊 PERFORMANCE SUMMARY")
        self.logger.info("="*60)
        self.logger.info(f"🔧 Total operations: {summary['total_operations']}")
        self.logger.info(f"✅ Successful: {summary['successful_operations']}")
        self.logger.info(f"❌ Failed: {summary['failed_operations']}")
        self.logger.info(f"⏱️  Total time: {summary['total_duration_seconds']:.2f}s")
        self.logger.info(f"📈 Average time per operation: {summary['average_duration_seconds']:.2f}s")
        
        if summary['total_records_processed']:
            self.logger.info(f"📊 Total records processed: {summary['total_records_processed']:,}")
            self.logger.info(f"🚀 Overall throughput: {summary['overall_records_per_second']:,.0f} records/sec")
        
        if summary['memory_usage']['peak_mb']:
            self.logger.info(f"🧠 Peak memory usage: {summary['memory_usage']['peak_mb']:.1f}MB")
        
        # Show top operations by duration
        operations_by_duration = sorted(
            [m for m in self.metrics if m.duration],
            key=lambda x: x.duration,
            reverse=True
        )[:5]
        
        if operations_by_duration:
            self.logger.info("\n🏆 TOP OPERATIONS BY DURATION:")
            for i, op in enumerate(operations_by_duration, 1):
                status = "✅" if op.success else "❌"
                self.logger.info(f"  {i}. {status} {op.operation_name}: {op.duration:.2f}s")

# Global performance monitor instance
performance_monitor = PerformanceMonitor()
