# matcher/reader.py

import pandas as pd
from io import BytesIO
from .models import TableProfile, ColumnSchema

# 엑셀 table 시트의 DATA_TYPE 값을 내부 타입 카테고리로 매핑
TYPE_CATEGORIES = {
    "STRING": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "TEXT": "STRING",

    "NUMERIC": "NUMERIC",
    "NUMBER": "NUMERIC",
    "INT": "NUMERIC",
    "INTEGER": "NUMERIC",
    "FLOAT": "NUMERIC",
    "DOUBLE": "NUMERIC",
    "DECIMAL": "NUMERIC",

    "DATE": "DATE",
    "DATETIME": "DATE",   # DATETIME은 DATE 카테고리로 통합
    "TIMESTAMP": "DATE",  # TIMESTAMP도 DATE 카테고리로 통합
}


def normalize_type(type_str: str) -> str:
    """엑셀 Data_Type 문자열을 내부 카테고리로 변환"""
    if type_str is None:
        return "STRING"
    t = str(type_str).strip().upper()
    return TYPE_CATEGORIES.get(t, "STRING")


def read_excel_to_profile(file_bytes: bytes, table_name: str) -> TableProfile:
    """
    업로드된 엑셀(bytes)을 받아서
    - table 시트: 스키마 정의 (COLUMN_NAME, DATA_TYPE, [TABLE_NAME])
    - sample 시트: 샘플 데이터
    를 읽고 TableProfile로 변환
    """
    xls = pd.ExcelFile(BytesIO(file_bytes))

    # 1) table 시트 읽기
    df_table = pd.read_excel(xls, sheet_name="table")

    # 헤더를 전부 대문자로 맞춰서 대소문자 혼용 방지
    df_table.columns = [str(c).upper() for c in df_table.columns]

    required_cols = {"COLUMN_NAME", "DATA_TYPE"}
    if not required_cols.issubset(set(df_table.columns)):
        raise ValueError("table 시트에 COLUMN_NAME, DATA_TYPE 컬럼이 필요합니다.")

    # 🔹 TABLE_NAME 컬럼이 있으면, 그 값으로 테이블 이름 자동 세팅
    effective_table_name = table_name  # 기본값 (파라미터로 들어온 값)
    if "TABLE_NAME" in df_table.columns:
        non_null_names = (
            df_table["TABLE_NAME"]
                .dropna()
                .astype(str)
                .str.strip()
        )
        if not non_null_names.empty:
            effective_table_name = non_null_names.iloc[0]

    # 2) sample 시트 읽기 (여기서는 원래 컬럼명 그대로 사용)
    df_sample = pd.read_excel(xls, sheet_name="sample")

    row_count = len(df_sample)
    columns: list[ColumnSchema] = []

    for _, row in df_table.iterrows():
        col_name = row["COLUMN_NAME"]                    # 실제 컬럼명 (엑셀에 적힌 그대로)
        raw_type_str = str(row["DATA_TYPE"]).strip()     # 원본 타입 문자열 (대소문자 보존용)
        raw_type_upper = raw_type_str.upper()            # normalize용

        # 기본 타입 매핑 (STRING / NUMERIC / DATE)
        col_type = normalize_type(raw_type_upper)

        # 샘플에 해당 컬럼이 있을 때만 추가 변환 로직 수행
        if col_name in df_sample.columns:
            series = df_sample[col_name]

            # 🔹 1) 컬럼명이 날짜스러운지 체크
            col_name_upper = str(col_name).upper()
            is_date_like_name = (
                col_name_upper.endswith("DATE")
                or col_name_upper.endswith("_DT")
                or col_name_upper.endswith("_DTTM")
            )

            # 🔹 2) DATETIME/TIMESTAMP → DATE (기존 룰)
            if is_date_like_name and raw_type_upper in {"DATETIME", "TIMESTAMP"}:
                try:
                    df_sample[col_name] = (
                        pd.to_datetime(series, errors="coerce").dt.date
                    )
                    col_type = "DATE"
                    series = df_sample[col_name]
                except Exception:
                    # 변환 실패해도 전체 앱이 죽지 않도록 방어
                    pass

            # 🔹 3) STRING 이지만 YYYYMMDD 패턴이면 → DATE로 변환 (새 룰)
            elif is_date_like_name and raw_type_upper == "STRING":
                # 문자열로 캐스팅 후 공백 제거
                s_str = series.dropna().astype(str).str.strip()
                # 숫자 8자리 (YYYYMMDD) 패턴인지 체크
                if len(s_str) > 0:
                    numeric8_mask = s_str.str.fullmatch(r"\d{8}")
                    # 샘플 값 중 80% 이상이 YYYYMMDD 형식이면 날짜로 본다
                    if numeric8_mask.mean() >= 0.8:
                        try:
                            # 전체 시리즈를 YYYYMMDD로 파싱 → date로 변환
                            df_sample[col_name] = (
                                pd.to_datetime(
                                    series.astype(str).str.strip(),
                                    format="%Y%m%d",
                                    errors="coerce",
                                ).dt.date
                            )
                            col_type = "DATE"
                            series = df_sample[col_name]
                        except Exception:
                            pass

            # 🔹 변환 후 최종 series 기준으로 통계 계산
            series = df_sample[col_name]
            distinct_count = series.nunique(dropna=True)
            null_ratio = series.isna().mean()
            sample_values = series.dropna().head(5).tolist()
        else:
            distinct_count = None
            null_ratio = None
            sample_values = []

        # 👉 여기서 raw_data_type에 "엑셀에서 읽은 원본 타입"을 그대로 넣어둔다
        columns.append(
            ColumnSchema(
                name=col_name,
                data_type=col_type,          # 내부 normalize된 타입 (STRING / NUMERIC / DATE)
                raw_data_type=raw_type_str,  # 엑셀 스키마의 원본 타입 (예: DATETIME, STRING 등)
                nullable=None,
                distinct_count=distinct_count,
                null_ratio=null_ratio,
                sample_values=sample_values,
            )
        )

    return TableProfile(
        table_name=effective_table_name,
        columns=columns,
        sample_df=df_sample,
        row_count=row_count,
    )
