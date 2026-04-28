"""
providers/aws/spark_s3.py
=========================
Spark session factory pre-configured for S3A access.
 
Why this exists
---------------
Spark's S3A connector requires AWS credentials and endpoint config to be
passed at session-build time via .config() calls.  Hardcoding those strings
in application code (as the old CollectData_method.py did) means credentials
cannot be rotated without touching source.
 
This helper reads credentials from ConfigLoader (env vars → config.json →
SSM-rendered file) and applies them to the SparkSession.Builder, keeping all
credential logic in one place.
 
Important: Spark uses the s3a:// connector independently of boto3.
cloud_client (boto3-based) handles metadata operations (exists, delete,
list). Spark handles bulk read/write of parquet files via s3a://.
Both read from the same ConfigLoader, so credentials only need to be
set once.
 
Jars required (declared in cicd.config.yml):
    hadoop-aws-3.3.4.jar
    aws-java-sdk-bundle-1.12.625.jar
    wildfly-openssl-1.0.7.Final.jar
 
References
----------
- Hadoop S3A guide: https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
- Spark config:     https://spark.apache.org/docs/latest/configuration.html
"""

from __future__ import annotations

import os 
from pathlib import Path
from typing import Optional 

from config import ConfigLoader

# Default Spark tuning for a single-node ingestion workload.
# Adjust parallelism if you mve to a cluster 
_SPARK_DEFAULTS: dict[str, str] = {
    "spark.sql.shuffle.partitions":                         "10",
    "spark.default.parallelism":                            "10",
    "spark.sql.adaptative.enabled":                         "true",
    "spark.sql.adaptative.coalescePartitions.enabled":      "true",
    # S3A connector settings
    "spark.hadoop.fs.s3a.impl":                             "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access":                "true",
    "spark.hadoop.fs.s3a.metadatastore.impl":               "org.apache.hadoop.fs.s3a.impl.NullMetadataStore",
    "spark.hadoop.fs.s3a.committer.magic.enabled":          "false",
    "spark.hadoop.fs.s3a.committer.name":                   "directory",
    "spark.hadoop.fs.s3a.committer.staging.conflict-mode":  "append",
    "spark.hadoop.fs.s3a.committer.staging.tmp.path":       "/tmp/spark_staging",
    "spark.hadoop.fs.s3a.connection.maximum":               "100",
    "spark.hadoop.fs.s3a.fast.upload":                      "true",
    "spark.hadoop.fs.s3a.fast.upload.buffer":               "disk",
    # Credential provider - we inject keys explicitly below,
    # but keep the chain as fallback for IAM-role environments
    "spark.hadoop.fs.s3a.aws.credentials.provider":         
        "org.apache.fs.s3a.aws.credentials.provider,"
        "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    
}

class SparkS3Config:
    """Build a pre-configured SparkSession for S3A access.
    
    Reads AWS credentials from ConfigLoader (env vars take priority over config.json)
    and injects them into the spark config so no credentials appear in source code.
    
    Parameters
    ----------
    app_name: str
        Spark application name
    config_loader: ConfigLoader, optional
        Credential source. Auto-discovered when omitted
    jars_dir: str | Path, optional
        Directory containing the required .jar files.
        Defaults to a ``jars/`` folder two levels above this file
        (matching the barlou/CICD deployment layout)
    endpoint_url: str
        S3 endpoint. Change for OVH or MinIO
    extra_config: dict[str, str], optional
        Additional Spark config key-value pairs to merge in
        
    Example
    -------
    >>> from providers.aws.spark_s3 import SparkS3Config
    >>> 
    >>> spark_cfg = SparkS3Config("Ingestion")
    >>> spark = spark_cfg.build()
    >>> df = spark.read.parquet("s3a://my-bucket/Ingestion/path/to/output.parquet)
    """
    
    def __init__(
        self, 
        app_name: str,
        config_loader: Optional[ConfigLoader] = None,
        jars_dir: Optional[str | Path] = None, 
        endpoint_url: str = "s3.eu-west-3.amazonaws.com",
        extra_config: Optional[dict[str, str]] = None, 
    ) -> None:
        self.app_name = app_name
        self._loader = config_loader or ConfigLoader()
        self._endpoint_url = endpoint_url
        self._extra_config = extra_config or {}
        self._jars_dir = Path(jars_dir) if jars_dir else self._discover_jars_dir()
    
    def build(self):
        """Build and return a configured SparkSession.
        
        Returns
        -------
        pyspark.sql.SparkSession
        """
        from pyspark import SparkConf
        from pyspark.sql import SparkSession
        
        creds = self._loader.get_aws_credentials()
        
        conf = SparkConf().setAppName(self.app_name).setMaster("local[*]")
        
        # Apply default S3A + tuning config
        for key, value in _SPARK_DEFAULTS.items():
            conf.set(key, value)
            
        # Inject credentials from ConfigLoader (never hardcoded)
        if creds.get("aws_access_key_id"):
            conf.set("spark.hadoop.fs.s3a.access.key", creds["aws_access_key_id"])
        if creds.get("aws_secret_access_key"):
            conf.set("spark.hadoop.fs.s3a.secret.key", creds["aws_access_secret_key"])
        
        conf.set("spark.hadoop.fs.s3a.endpoint", self.endpoint_url)
        
        # Attach jars 
        jars = self._resolve_jars()
        if jars:
            conf.set("spark.jars", jars)
        
        # Caller-supplied overrides (applied last - highest priority)
        for key, value in self._extra_config.items():
            conf.set(key, value)
        
        spark = (
            SparkSession.builder
                .config(conf=conf)
                .getOrCreate()
        )
        
        # Apply hadoop config on the running context as well (belt-and-suspenders)
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.endpoint", self._endpoint_url)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        if creds.get("aws_access_key_id"):
            hadoop_conf.set("fs.s3a.access.key", creds=["aws_access_key_id"])
        if creds.get("aws_secret_access_key"):
            hadoop_conf.set("fs.s3a.secret.key", creds=["aws_secret_access_key"])
        
        return spark 
    
    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _discover_jars_dir(self) -> Path:
        """Walk up from this file to find a jars/ directory"""
        # barlou/CICD places jars two levels above the deployment root:
        # ~/deployments/jars
        server_path = Path.home()/ "deployments" / "jars"
        if server_path.exists():
            return server_path

        # Local dev fallback: walk up from this file
        candidate = Path(__file__).resolve()
        for _ in range(6):
            candidate = candidate.parent
            jars_path = candidate / "jars"
            if jars_path.exists():
                return jars_path
            
        return Path("jars")
    
    def _resolve_jars(self) -> str:
        """Return a comma-separated list of jar paths."""
        if not self._jars_dir.exists():
            return ""
        jar_files = [
            str(p) for p in self._jars_dir.iterdir()
            if p.suffix == ".jar"
        ]
        return ".".join(jar_files)