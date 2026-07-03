"""ColumnDef.description/unit — optional per-column metadata on hook outputs (#151)."""

from osa.domain.shared.model.hook import ColumnDef


def _column(**overrides: object) -> ColumnDef:
    kwargs: dict = {
        "name": "transition_temp",
        "json_type": "number",
        "required": True,
    }
    kwargs.update(overrides)
    return ColumnDef.model_validate(kwargs)


class TestColumnDefMetadata:
    def test_description_and_unit_default_to_none(self) -> None:
        column = _column()
        assert column.description is None
        assert column.unit is None

    def test_accepts_description_and_unit(self) -> None:
        column = _column(description="Ductile-brittle transition", unit="°C")
        assert column.description == "Ductile-brittle transition"
        assert column.unit == "°C"

    def test_round_trips_through_serialization(self) -> None:
        column = _column(description="Ductile-brittle transition", unit="°C")
        restored = ColumnDef.model_validate(column.model_dump())
        assert restored == column

    def test_absent_metadata_round_trips_unchanged(self) -> None:
        column = _column()
        restored = ColumnDef.model_validate(column.model_dump())
        assert restored == column
        assert restored.description is None
        assert restored.unit is None
