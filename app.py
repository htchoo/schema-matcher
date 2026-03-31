import streamlit as st
import pandas as pd
import io
from graphviz import Digraph
from itertools import combinations

# 라이브러리 임포트 (기존 환경 유지)
from matcher.reader import read_excel_to_profile
from matcher.matcher import match_schemas
from matcher.join_detector import detect_join_keys
from matcher.profiler import summarize_table_profile

# ==========================================
# 🔐 보안 모듈 (수정 금지 영역)
# ==========================================
def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["APP_ACCESS_PASSWORD"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.title("🔒 SQL Agent 보안 접속")
        st.text_input("접근 암호를 입력하세요 (Access Key)", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.title("🔒 SQL Agent 보안 접속")
        st.text_input("접근 암호를 입력하세요 (Access Key)", type="password", on_change=password_entered, key="password")
        st.error("😕 암호가 올바르지 않습니다.")
        return False
    return True

if not check_password():
    st.stop()

# 1. 페이지 설정
st.set_page_config(page_title="스키마 탐지기 Pro", layout="wide")
st.title("🔍 테이블 스키마 탐지 & Join Key 분석기")

# ---------------------------------------------------------
# [데이터 정제] 전수 분석용
# ---------------------------------------------------------
def clean_df(df):
    if df is None: return None
    df = df.dropna(how='all').copy()
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip().str.upper()
    return df

@st.cache_data(show_spinner=False)
def get_cached_profile(file_bytes, table_id, sheet_name):
    profile = read_excel_to_profile(file_bytes, table_id)
    full_df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)
    profile.sample_df = clean_df(full_df)
    return profile

# ---------------------------------------------------------
# [로직 변경] 전략적 PK 분석 (Best Effort 방식 적용)
# ---------------------------------------------------------
def analyze_strategic_pks(df):
    if df is None or df.empty: return pd.DataFrame(), 0
    total_rows = len(df)
    target_cols = [col for col in df.columns if df[col].notnull().any()]
    results = []
    
    # 1. 단일 PK 검사 (100% 확정 탐색)
    for col in target_cols:
        nunique = df[col].nunique()
        if nunique == total_rows:
            results.append({"구분": "단일 PK", "컬럼 1": col, "컬럼 2": "-", "컬럼 3": "-", "유일성(%)": 100.0, "상태": "✅ 확정"})
    
    # 단일 PK 확정 건이 있으면 해당 결과만 반환 (기존 로직 유지)
    if results: 
        return pd.DataFrame(results), total_rows

    # 2. [신규 로직] 확정 건이 없을 경우 후보군 분석 (Best Effort)
    # 유일성 수치가 높은 상위 10개 컬럼 추출
    top_potential = sorted(target_cols, key=lambda c: df[c].nunique(), reverse=True)[:10]
    
    for col in top_potential:
        nunique = df[col].nunique()
        uniqueness = round((nunique / total_rows) * 100, 2)
        results.append({
            "구분": "단일 PK", "컬럼 1": col, "컬럼 2": "-", "컬럼 3": "-", 
            "유일성(%)": uniqueness, "상태": "⚠️ 후보"
        })
        
    # 복합 PK 조합 분석 (2개 조합 중 유일성 80% 이상인 것들)
    for combo in combinations(top_potential, 2):
        distinct_count = len(df.groupby(list(combo)).size())
        uniqueness = round((distinct_count / total_rows) * 100, 2)
        if uniqueness > 80:
            results.append({
                "구분": "복합 PK (2개)", "컬럼 1": combo[0], "컬럼 2": combo[1], 
                "컬럼 3": "-", "유일성(%)": uniqueness, "상태": "⚠️ 후보"
            })
    
    # 유일성 높은 순으로 정렬 후 상위 10개만 표시
    res_df = pd.DataFrame(results).sort_values(by="유일성(%)", ascending=False).head(10).reset_index(drop=True)
    return res_df, total_rows

# ---------------------------------------------------------
# 2. 사이드바 (레이아웃 유지)
# ---------------------------------------------------------
st.sidebar.header("⚙️ 분석 설정")
w_name = st.sidebar.number_input("Name Similarity", 0.0, 1.0, 0.30, 0.05)
w_type = st.sidebar.number_input("Type Similarity", 0.0, 1.0, 0.10, 0.05)
w_value = st.sidebar.number_input("Value Overlap", 0.0, 1.0, 0.60, 0.05)
weights_tuple = (w_name, w_type, w_value)

st.sidebar.markdown("---")
rel_type_input = st.sidebar.radio("테이블 관계 설정", ["자동 탐지", "1:1", "1:N", "N:1"], index=0)

# ---------------------------------------------------------
# 3. 메인 레이아웃 (레이아웃 유지)
# ---------------------------------------------------------
col_up1, col_up2 = st.columns(2)
with col_up1:
    file_a = st.file_uploader("소스(Source) 테이블 업로드", type=["xlsx"], key="file_a")
    if file_a:
        xl_a = pd.ExcelFile(io.BytesIO(file_a.getvalue()))
        sheet_a = st.selectbox("분석 시트 선택 (Source)", xl_a.sheet_names, index=len(xl_a.sheet_names)-1)

with col_up2:
    file_b = st.file_uploader("타겟(Target) 테이블 업로드 (선택)", type=["xlsx"], key="file_b")
    if file_b:
        xl_b = pd.ExcelFile(io.BytesIO(file_b.getvalue()))
        sheet_b = st.selectbox("분석 시트 선택 (Target)", xl_b.sheet_names, index=len(xl_b.sheet_names)-1)

if file_a:
    try:
        profile_a = get_cached_profile(file_a.getvalue(), "SRC", sheet_a)
        df_source, _ = summarize_table_profile(profile_a)
        full_nunique_map_a = {col: profile_a.sample_df[col].nunique() for col in profile_a.sample_df.columns}
        row_count_a = len(profile_a.sample_df)
        
        # 소스 PK 탐색
        df_pk_res_a, _ = analyze_strategic_pks(profile_a.sample_df)
        # 결과가 있으면 첫 번째 행의 컬럼을 대표 PK로 설정
        pk_a = df_pk_res_a.iloc[0]["컬럼 1"] if not df_pk_res_a.empty else "MODEL_CODE"

        st.success(f"전수 분석 완료 (유효 행 수: {row_count_a}) ✅")
        st.subheader("1. 소스 테이블 스키마 및 데이터 분석")
        c_main, c_side = st.columns([2, 1])
        with c_main:
            st.markdown("**📊 필드별 데이터 프로파일 요약**")
            df_source["Distinct"] = df_source["Column"].map(full_nunique_map_a).fillna(0).astype(int)
            st.dataframe(df_source.sort_values(by="Distinct", ascending=False).style.apply(
                lambda r: ['background-color: #D1E9F6']*len(r) if r["Column"] == pk_a else ['']*len(r), axis=1
            ), use_container_width=True)
        with c_side:
            st.markdown("**📌 PK 후보 데이터 중복 상세 분석**")
            dupes = profile_a.sample_df[pk_a].value_counts()
            st.table(pd.DataFrame({"중복 데이터": dupes[dupes>1].index, "건수": dupes[dupes>1].values}).head(10))

        st.markdown("### 📊 상세 PK 분석 결과")
        st.table(df_pk_res_a)

        # ---------------------------------------------------------
        # 4. 소스-타겟 매칭 분석 및 ERD/SQL
        # ---------------------------------------------------------
        if file_b:
            profile_b = get_cached_profile(file_b.getvalue(), "TGT", sheet_b)
            all_matches = match_schemas(profile_a, profile_b, weights=weights_tuple)
            
            row_count_b = len(profile_b.sample_df)
            full_nunique_map_b = {col: profile_b.sample_df[col].nunique() for col in profile_b.sample_df.columns}
            
            # 타겟 PK 탐색
            df_pk_res_b, _ = analyze_strategic_pks(profile_b.sample_df)
            pk_b = df_pk_res_b.iloc[0]["컬럼 1"] if not df_pk_res_b.empty else "ITEM_ID"

            for m in all_matches:
                a_col, b_col = m['A_column'], m['B_column']
                is_pk_a = (full_nunique_map_a.get(a_col, 0) == row_count_a)
                is_pk_b = (full_nunique_map_b.get(b_col, 0) == row_count_b)
                if is_pk_a and is_pk_b: m['relationship_detected'] = "1:1"
                elif is_pk_a and not is_pk_b: m['relationship_detected'] = "1:N"
                elif not is_pk_a and is_pk_b: m['relationship_detected'] = "N:1"
                else: m['relationship_detected'] = "N:M"
                if a_col == b_col and a_col == 'MODEL_CODE':
                    m['match_type'] = 'strong'; m['score'] = 1.0; m['value_overlap'] = 1.0

            filtered_matches = [m for m in all_matches if m.get('match_type') != 'weak']
            top_m = sorted(filtered_matches, key=lambda x: x['score'], reverse=True)[0]
            rel_final = rel_type_input if rel_type_input != "자동 탐지" else top_m['relationship_detected']
            
            st.subheader(f"2. 소스 ↔ 타겟 매칭 추천 결과 (시스템이 탐지한 관계: {rel_final})")
            st.dataframe(pd.DataFrame(filtered_matches)[['A_column', 'B_column', 'score', 'relationship_detected', 'match_type']].sort_values(by="score", ascending=False), use_container_width=True)

            st.subheader("3. JOIN KEY 후보 및 ERD 분석 결과")
            dot = Digraph(comment="ERD"); dot.attr(rankdir="LR")
            
            def make_node(t_name, pk_col, join_col):
                display_cols = [pk_col]
                if join_col != pk_col: display_cols.append(join_col)
                rows = "".join([f'<TR><TD ALIGN="LEFT" BGCOLOR="{"#D1E9F6" if c==join_col else "white"}">{"🔑 " if c==pk_col else "   "}{c}</TD></TR>' for c in display_cols])
                return f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0"><TR><TD BGCOLOR="#F4B2AF"><B>{t_name}</B></TD></TR>{rows}</TABLE>>'

            if rel_final == "N:1":
                dot.node("N", label=make_node(profile_a.table_name, pk_a, top_m['A_column']), shape="plaintext")
                dot.node("1", label=make_node(profile_b.table_name, pk_b, top_m['B_column']), shape="plaintext")
                dot.edge("N", "1", label="N:1", arrowhead="none", arrowtail="crow", dir="both")
                sql = f"SELECT\n    A.{pk_a},\n    A.{top_m['A_column']},\n    B.{pk_b}\nFROM {profile_a.table_name} A\nLEFT JOIN {profile_b.table_name} B\n    ON A.{top_m['A_column']} = B.{top_m['B_column']}\nWHERE A.{top_m['A_column']} IS NOT NULL;"
            elif rel_final == "1:N":
                dot.node("N", label=make_node(profile_b.table_name, pk_b, top_m['B_column']), shape="plaintext")
                dot.node("1", label=make_node(profile_a.table_name, pk_a, top_m['A_column']), shape="plaintext")
                dot.edge("N", "1", label="N:1", arrowhead="none", arrowtail="crow", dir="both")
                sql = f"SELECT\n    A.{pk_b},\n    A.{top_m['B_column']},\n    B.{pk_a}\nFROM {profile_b.table_name} A\nLEFT JOIN {profile_a.table_name} B\n    ON A.{top_m['B_column']} = B.{top_m['A_column']}\nWHERE A.{top_m['B_column']} IS NOT NULL;"
            else: # 1:1
                dot.node("L", label=make_node(profile_a.table_name, pk_a, top_m['A_column']), shape="plaintext")
                dot.node("R", label=make_node(profile_b.table_name, pk_b, top_m['B_column']), shape="plaintext")
                dot.edge("L", "R", label="1:1", dir="both")
                sql = f"SELECT\n    A.{pk_a},\n    B.{pk_b}\nFROM {profile_a.table_name} A\nINNER JOIN {profile_b.table_name} B\n    ON A.{top_m['A_column']} = B.{top_m['B_column']};"

            st.graphviz_chart(dot)
            st.markdown(f"**📜 비즈니스 로직 기반 SQL 쿼리 추천 (적용 관계: {rel_final})**")
            st.code(sql, language="sql")
            
    except Exception as e:
        st.error(f"오류: {e}")