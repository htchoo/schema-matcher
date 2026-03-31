# matcher/join_detector.py

from .models import TableProfile, ColumnSchema
from .matcher import value_overlap_score


def is_pk_candidate(column: ColumnSchema, row_count: int) -> bool:
    """
    샘플 기준 PK 후보:
    - distinct 비율 >= 0.9
    - null 비율 < 0.05
    """
    if column.distinct_count is None or row_count == 0:
        return False

    distinct_ratio = column.distinct_count / row_count
    null_ratio = column.null_ratio or 0.0
    return distinct_ratio >= 0.9 and null_ratio < 0.05


def _classify_relationship(col_a: str, col_b: str, pk_a: set[str], pk_b: set[str]) -> str:
    """
    PK 후보 집합을 기준으로 관계 라벨링:
      - 1:1
      - 1:N (A→B)
      - N:1 (A→B)
      - N:M
    """
    if col_a in pk_a and col_b in pk_b:
        return "1:1"
    elif col_a in pk_a and col_b not in pk_b:
        return "1:N (A→B)"
    elif col_a not in pk_a and col_b in pk_b:
        return "N:1 (A→B)"
    else:
        return "N:M"


def detect_join_keys(
    table_a: TableProfile,
    table_b: TableProfile,
    matches: list[dict],
):
    """
    컬럼 매칭 결과(matches)를 기반으로 Join Key 후보를 탐지한다.

    규칙 요약:
    1) match_type == "strong" 인 컬럼 매칭만 사용
    2) 값 overlap이 일정 이상(기본 0.5 이상)인 경우만 고려
    3) 양쪽 PK 후보 여부를 바탕으로 관계 라벨만 지정:
         - 1:1
         - 1:N (A→B)
         - N:1 (A→B)
         - N:M
       N:M도 포함해서 모두 Join Key 후보로 반환한다.
    4) 정렬 우선순위:
         - 1:1 → 1:N → N:1 → N:M
         - 같은 관계 안에서는 overlap이 높은 순
    """
    candidates = []

    # 각 테이블에서 PK 후보 컬럼 집합
    pk_a = {c.name for c in table_a.columns if is_pk_candidate(c, table_a.row_count)}
    pk_b = {c.name for c in table_b.columns if is_pk_candidate(c, table_b.row_count)}

    for m in matches:
        # 1) strong 매칭만 Join 후보로 고려
        if m.get("match_type") != "strong":
            continue

        col_a = m["A_column"]
        col_b = m["B_column"]

        # 2) 값 overlap 계산
        overlap = value_overlap_score(
            table_a.sample_df, col_a,
            table_b.sample_df, col_b,
        )

        # 값이 거의 안 겹치면 조인 키로 보기 어렵다고 판단
        if overlap < 0.5:
            continue

        # 3) 관계 라벨링
        rel = _classify_relationship(col_a, col_b, pk_a, pk_b)

        candidates.append(
            {
                "A_column": col_a,
                "B_column": col_b,
                "overlap": round(overlap, 3),
                "relationship": rel,
            }
        )

    # 4) 정렬: 관계 우선 + overlap 높은 순
    rel_order = {
        "1:1": 0,
        "1:N (A→B)": 1,
        "N:1 (A→B)": 2,
        "N:M": 3,
    }

    candidates.sort(
        key=lambda x: (
            rel_order.get(x["relationship"], 99),
            -x["overlap"],
        )
    )

    return candidates
