from __future__ import annotations

from pathlib import Path


CSV_TEMPLATE_COLUMNS = ["date", "open", "high", "low", "close", "adj_close", "volume"]


def create_csv_template(*, symbol: str, output_path: Path) -> dict:
    if not output_path.name.endswith("_template.csv"):
        raise ValueError("CSV templates must use *_template.csv suffix so dataset builds ignore them")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        CSV_TEMPLATE_COLUMNS,
        ["2020-01-02", "100.00", "101.00", "99.50", "100.50", "100.50", "1000000"],
        ["2020-01-03", "100.50", "102.00", "100.00", "101.25", "101.25", "1100000"],
        ["2020-01-06", "101.25", "101.75", "100.75", "101.00", "101.00", "900000"],
    ]
    output_path.write_text("\n".join(",".join(row) for row in rows) + "\n", encoding="utf-8")
    return {
        "symbol": symbol.upper(),
        "path": str(output_path),
        "columns": CSV_TEMPLATE_COLUMNS,
        "template": True,
        "do_not_import_template": True,
    }

