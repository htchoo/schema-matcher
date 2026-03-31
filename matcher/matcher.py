import re
from rapidfuzz import fuzz
from .models import TableProfile

def normalize_colname(name: str) -> str:
    """컬럼명 비교용 정규화 (특수문자 제거 및 대문자화)"""
    return re.sub(r"[^A-Z0-9]", "", str(name).upper())

def name_similarity(a: str, b: str) -> float:
    """0~1 스케일 문자열 유사도 (Token Sort Ratio 사용)"""
    return fuzz.token_sort_ratio(a, b) / 100.0

def type_compatible(type_a: str, type_b: str) -> float:
    """데이터 타입 호환성 점수"""
    if type_a == type_b:
        return 1.0
    # 문자열과 숫자 간의 매핑 가능성은 낮게 평가
    if {type_a, type_b} == {"STRING", "NUMERIC"}:
        return 0.3
    return 0.0

def value_overlap_score(df_a, col_a, df_b, col_b) -> float:
    """샘플 데이터 값의 실제 겹침 비율 (최대 200개 샘플 기준)"""
    if df_a is None or df_b is None:
        return 0.0
    if col_a not in df_a.columns or col_b not in df_b.columns:
        return 0.0

    vals_a = set(df_a[col_a].dropna().astype(str).head(200))
    vals_b = set(df_b[col_b].dropna().astype(str).head(200))
    
    if not vals_a or not vals_b:
        return 0.0

    inter = vals_a & vals_b
    # 작은 집합 크기 대비 교집합의 비율로 계산 (포함 관계 확인)
    return len(inter) / min(len(vals_a), len(vals_b))

def match_schemas(table_a: TableProfile, table_b: TableProfile, weights=(0.6, 0.3, 0.1)):
    """
    테이블 A의 각 컬럼에 대해 테이블 B의 최적 매칭 컬럼을 찾음.
    
    Args:
        table_a: 소스 테이블 프로필
        table_b: 타겟 테이블 프로필
        weights: (이름유사도 가중치, 타입가중치, 값중첩가중치) 튜플
    """
    results = []
    
    # 전달받은 가중치 언패킹
    w_name, w_type, w_val = weights

    for col_a in table_a.columns:
        best_match = None
        
        # 소스 테이블 컬럼 정규화 (반복문 밖에서 한 번만 수행)
        norm_a = normalize_colname(col_a.name)

        for col_b in table_b.columns:
            # 1. 이름 유사도 계산
            name_sim = name_similarity(norm_a, normalize_colname(col_b.name))
            
            # 2. 타입 호환성 계산
            type_sim = type_compatible(col_a.data_type, col_b.data_type)
            
            # 3. 데이터 값 중첩도 계산
            val_sim = value_overlap_score(
                table_a.sample_df, col_a.name,
                table_b.sample_df, col_b.name,
            )

            # [핵심 수정] 전달받은 가중치를 적용하여 최종 점수 산출
            score = (w_name * name_sim) + (w_type * type_sim) + (w_val * val_sim)

            result = {
                "A_column": col_a.name,
                "B_column": col_b.name,
                "score": round(score, 3),
                "name_similarity": round(name_sim, 3),
                "type_similarity": round(type_sim, 3),
                "value_overlap": round(val_sim, 3),
            }

            # 최적의 매칭쌍 업데이트
            if best_match is None or score > best_match["score"]:
                best_match = result

        # 매칭 등급 분류 (최종 점수 기준)
        if best_match:
            if best_match["score"] >= 0.85:
                best_match["match_type"] = "strong"
            elif best_match["score"] >= 0.6:
                best_match["match_type"] = "candidate"
            else:
                best_match["match_type"] = "weak"
            
            results.append(best_match)

    return results