"""Read validated CSV / XLSX files into pandas DataFrames.

Pandas is used for the pre-analysis stages (preview, mapping detection,
validation, normalization). PySpark takes over for the segmentation pipeline
in a later phase. Handles encodings and CSV delimiters where practical.
"""
import logging

import pandas as pd

log = logging.getLogger("segmentation.filereader")

# Common encodings tried in order for CSV files.
_ENCODINGS = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]


class FileReaderError(Exception):
    pass


def read_dataframe(path: str, extension: str, sheet: str | None = None) -> pd.DataFrame:
    """Load a file into a DataFrame of raw (string-preserving) values.

    For CSVs the delimiter is sniffed and encodings are tried in order. All
    columns are read as strings to avoid pandas silently coercing values
    (e.g. turning "N/A" text or leading-zero IDs into numbers) before the
    mapping/validation stages can inspect them.
    """
    if extension == "csv":
        return _read_csv(path)
    if extension == "xlsx":
        return _read_xlsx(path, sheet)
    raise FileReaderError(f"Unsupported file extension for reading: {extension}")


def _read_csv(path: str) -> pd.DataFrame:
    last_error = None
    for enc in _ENCODINGS:
        try:
            sep = _sniff_delimiter(path, enc)
            df = pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False,
                             na_values=[], encoding=enc, engine="python", on_bad_lines="warn")
            return _clean_column_names(df)
        except (UnicodeDecodeError, pd.errors.ParserError) as exc:
            last_error = exc
            continue
        except pd.errors.EmptyDataError:
            raise FileReaderError("The CSV file contains no data.")
    raise FileReaderError(f"Could not read the CSV file: {last_error}")


def _sniff_delimiter(path: str, encoding: str) -> str:
    import csv as _csv

    try:
        with open(path, "r", encoding=encoding, newline="") as fh:
            sample = fh.read(64 * 1024)
            dialect = _csv.Sniffer().sniff(sample, delimiters=",;\t|")
            return dialect.delimiter
    except _csv.Error:
        return ","


def _read_xlsx(path: str, sheet: str | None) -> pd.DataFrame:
    try:
        df = pd.read_excel(path, sheet_name=sheet or 0, dtype=str)
    except ValueError as exc:
        raise FileReaderError(f"The requested worksheet was not found in the workbook: {exc}")
    except Exception as exc:
        raise FileReaderError(f"Could not read the Excel file: {exc}")
    return _clean_column_names(df)


def _clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from column names and de-duplicate collisions."""
    rename, seen = {}, {}
    for col in df.columns:
        name = str(col).strip() or "column"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        rename[col] = name
    df = df.rename(columns=rename)
    # Replace empty-string cells with NaN for consistent downstream handling.
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    return df


def list_sheets(path: str) -> list[str]:
    from app.services.file_validation_service import list_excel_sheets

    return list_excel_sheets(path)
