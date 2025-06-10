"""
HDFS utilities package.

This package provides HDFS operations with proper configuration management
and dependency injection for maximum flexibility and reusability.
"""

from .config import (
    HDFSConfig,
    create_hdfs_config,
    create_local_config,
    create_cluster_config,
    create_geocoding_config,
    create_config_from_env
)

from .operations import (
    HDFSOperations,
    setup_distributed_environment,
    create_hdfs_operations
)

# Convenience functions for common patterns
def quick_setup(
    data_path: str,
    app_name: str = "UtilitiesApp",
    **kwargs
) -> tuple:
    """
    Ultra-quick setup for simple use cases.

    Args:
        data_path: Path to data directory
        app_name: Spark application name
        **kwargs: Additional configuration options

    Returns:
        Tuple of (spark_session, effective_data_path, dependencies_info)
    """
    config = create_local_config(
        data_path=data_path,
        app_name=app_name,
        **kwargs
    )
    return setup_distributed_environment(config)

__all__ = [
    # Configuration classes and factories
    "HDFSConfig",
    "create_hdfs_config",
    "create_local_config",
    "create_cluster_config",
    "create_geocoding_config",
    "create_config_from_env",

    # Operations classes and functions
    "HDFSOperations",
    "setup_distributed_environment",
    "create_hdfs_operations",

    # Convenience functions
    "quick_setup",
]
