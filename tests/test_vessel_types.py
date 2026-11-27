from oceanwatch import refs


def test_vessel_type_catalog_covers_0_to_99_once():
    rows = refs.vessel_type_rows()
    assert [code for code, _ in rows] == list(range(100))
    assert dict(rows)[37] == "Pleasure craft" and dict(rows)[75] == "Cargo"
