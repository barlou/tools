"""
config.py
=========
Credential and configuration loader for cloud_client.
 
Priority chain (highest → lowest)
----------------------------------
1. Environment variables     — CI/CD secrets, Docker env, Airflow connections
2. config.json on disk       — local dev, server-rendered from SSM by deploy.sh
 
On the server, barlou/CICD's deploy.sh renders config/config.json from
AWS SSM SecureString values and writes it with chmod 600.  The MODULE_NAME
env var (set by deploy.sh's deploy_env.sh) tells us where to find it:
    ~/deployments/{MODULE_NAME}/config/config.json
 
In local dev, the loader searches cwd and cwd/config/ for config.json.
In CI/CD test runs, env vars are injected as GitHub Secrets — no file needed.
 
References
----------
- 12-factor config:   https://12factor.net/config
- boto3 credentials:  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
- AWS SSM:            https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-parameter-store.html
"""

from __future__ import annotations

import os, json
from pathlib import Path
from typing import Any, Optional

from base import CloudConfigError

_CONFIG_FILENAMES = ("config.json", "cloud_config.json")

class ConfigLoader:
    """Load cloud credentials from environment variables or a JSON file
    
    Parameters
    ----------
    config_path: str | Path, optional
        Explicit path to a config.json file
        When omitted, the loader auto-discovers the file 
    
    Examples:
    >>> loader = ConfigLoader()
    >>> loader = ConfigLoader("cloud_client/config/config.json)
    >>> creds  = loader.get_aws_credentials()
    """
    
    def __init__(self, config_path: Optional[str | Path] = None) -> None:
        self._path: Optional[Path] = None
        self._data: dict[str, Any] = {}
        
        if config_path:
            self._path = Path(config_path).expanduser().resolve()
            if not self._path.exists():
                raise CloudConfigError(f"Config file not found: {self._path}")
            self._data = self._load_json(self._path)
        else:
            self._path = self._discover()
            if self._path:
                self._data = self._load_json(self._path)
                
    # ------------------------------------------------------------------
    # Public credential helpers
    # ------------------------------------------------------------------
    
    def get_aws_credentials(self) -> dict[str, Optional[str]]:
        """Return AWS credentials dict.
        
        Env-var names follow the official AWS SDK convention so boto3 picks
        them up automatically when they are set in the environment
        
        Returns
        -------
        dict with keys: aws_access_key_id, aws_secret_access_key, region_name
        """
        return {
            "aws_access_key_id": self._resolve(
                env_keys="AWS_ACCESS_KEY_ID",
                json_keys=["aws_access_key_id", "AWS_ACCESS_KEY"],
            ),
            "aws_secret_access_key": self._resolve(
                env_key="AWS_SECRET_ACCESS_KEY",
                json_keys=["aws_secret_access_key", "AWS_SECRET_KEY"],
            ),
            "region_name": self._resolve(
                env_key="AWS_DEFAULT_REGION",
                json_keys=["aws_region", "region"],
                required=False,
            ),
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Generic key lookup: env var -> JSON key -> default."""
        return os.environ.get(key) or self._data.get(key, default)
    
    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    
    def _resolve(
        self,
        *,
        env_key: str,
        json_keys: list[str],
        required: bool = True,
    ) -> Optional[str]:
        # 1. Environment variable (highest priority)
        value = os.environ.get(env_key)
        if value:
            return value
        
        # 2. Nested storage key in config.json
        storage = self._data.get("storage", {})
        for jk in json_keys:
            value = storage.get(jk) or self._data.get(jk)
            if value:
                return value
        
        if required:
            raise CloudConfigError(
                f"Missing credential. Set env var '{env_key}' "
                f"or add one of {json_keys} to config.json"
            )
        return None
    
    @staticmethod
    def _load_json(path: Path) -> dict[str, Any]:
        try:
            with path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except json.JSONDecodeError as exc:
            raise CloudConfigError(f"Invalid JSON in {path}: {exc}") from exc
    
    @staticmethod
    def _discover() -> Optional[Path]:
        """Search standard locations for config.json
        
        Order:
        1. Server deploy path: ~/deployments/{MODULE_NAME}/config/config.json
            (rendered by deploy.sh from SSM - MODULE_NAME set in deploy_env.sh)
        2. Current working directory
        3. cwd/config
        4. Package-relative config/ 
        """
        module_name = os.environ.get("MODULE_NAME", "")
        if module_name:
            server_path= (
                Path.home() / "deployments" / module_name / "config" / "config.json"
            )
            if server_path.exists():
                return server_path
        search_dirs = [
            Path.cwd(),
            Path.cwd() / "config",
            Path(__file__).parent.parent / "config",
        ]
        for directory in search_dirs:
            for name in _CONFIG_FILENAMES:
                candidate = directory / name
                if candidate.exists():
                    return candidate
        
        return None
    
    def __repr__(self) -> str:
        src = str(self._path) if self._path else "env-only"
        return f"ConfigLoader(source={src})"