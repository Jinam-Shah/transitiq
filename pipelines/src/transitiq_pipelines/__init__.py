"""TransitIQ data pipelines: ingestion into bronze, validation into silver.

Pure-stdlib core (http, storage, checkpoints) so the same code runs as a local CLI and inside
AWS Lambda. Heavy libraries (pyarrow, boto3) are imported lazily, only where needed.
"""

__version__ = "0.3.0"
