# matcher/profiler.py
import pandas as pd
from .models import TableProfile, ColumnSchema


def is_pk_candidate_for_profile(column: ColumnSchema, row_count: int) -> bool:
    """
    샘플 데이터 기준 PK 후보:
    - distinct 비율 >= 0.9
    - null 비율 < 0.05
    """
    if column.distinct_count is None or row_count == 0:
        return False

    distinct_ratio = column.distinct_count / row_count
    null_ratio = column.null_ratio or 0.0
    return distinct_ratio >= 0.9 and null_ratio < 0.05


def summarize_table_profile(profile: TableProfile, high_null_threshold: float = 0.5):
    """
    소스 테이블 1개를 컬럼 단위로 요약해주는 함수.

    반환값:
      - df: 컬럼별 요약이 담긴 pandas DataFrame
      - summary: {
            "row_count": int,
            "pk_candidates": [컬럼명 리스트],
            "high_null_cols": [컬럼명 리스트]
        }
    """
    rows = []
    pk_candidates = []
    high_null_cols = []

    for col in profile.columns:
        is_pk = is_pk_candidate_for_profile(col, profile.row_count)

        if is_pk:
            pk_candidates.append(col.name)

        if col.null_ratio is not None and col.null_ratio >= high_null_threshold:
            high_null_cols.append(col.name)

        rows.append(
            {
                "Column": col.name,
                "Type": col.data_type,
                "Distinct": col.distinct_count,
                "Null_Ratio": round(col.null_ratio, 3)
                if col.null_ratio is not None
                else None,
                "PK_Candidate": is_pk,
            }
        )

    df = pd.DataFrame(rows)

    # PK_Candidate = True 먼저, 그 안에서는 Null_Ratio 낮은 순,
    # 그 다음 컬럼 이름 알파벳 순. Null_Ratio가 높은 컬럼일수록 아래로 내려감.
    df = df.sort_values(
        by=["PK_Candidate", "Null_Ratio", "Column"],
        ascending=[False, True, True],  # PK True 먼저, Null 낮은 값 먼저
        na_position="last",             # Null_Ratio가 None인 경우 맨 아래
    )


    summary = {
        "row_count": profile.row_count,
        "pk_candidates": pk_candidates,
        "high_null_cols": high_null_cols,
    }

    return df, summary
