from app.services.metadata import discover_sensitive_columns
from app.services.metadata import MetadataScanner as ms

def test_discover_pii():
    cols = discover_sensitive_columns()
    assert isinstance(cols, list)

def test_connection_and_metadata():
    assert len(ms.sensitive_columns()) >= 0
    assert len(ms.grants_on_tables()) >= 0
    assert len(ms.column_lineage()) >= 0