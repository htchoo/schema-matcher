import streamlit as st
import pandas as pd
import io
from graphviz import Digraph
from itertools import combinations

# 라이브러리 임포트 (기존 환경 및 로직 유지)
from matcher.reader import read_excel_to_profile
from matcher.matcher import match_schemas
from matcher.join_detector import detect_join_keys
from matcher.profiler import summarize_table_profile

# ==========================================
# 🔐 보안 모듈 (절대 수정 금지)
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
# [데이터 정제 및 캐싱] - 전수 분석 보장
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
# [로직 고정] 카탈로그 실전 정밀 매핑 (English Name 기반 분석)
# ---------------------------------------------------------
def lookup_catalog_report(names, pjts, type='table'):
    results = []
    tbl_master = {
        "TRSALE_ORD_STAT_D": {"LG D2C": ("주문 현황 일적재", "일별 주문 및 판매 상태 트랜잭션 정보")},
        "VRCOMN_MODEL_INFO_M": {"LG D2C": ("모델 마스터 정보", "공용 제품 모델에 대한 표준 규격 마스터")}
    }
    col_master = {
        "MODEL_CODE": {"LG D2C": ("모델코드", "제품 식별 고유 8자리 코드")},
        "ITEM_ID": {"LG D2C": ("아이템ID", "주문 품목별 고유 식별 번호")},
        "CMPNY": {"LG D2C": ("사업본부", "LG/삼성 등 제조 법인 구분 정보")},
        "ORD_DATE": {"LG D2C": ("주문일자", "YYYYMMDD 형식의 실적 발생일")}
    }
    for name in names:
        found = False
        master = tbl_master if type == 'table' else col_master
        if name in master:
            for pjt in pjts:
                if pjt in master[name]:
                    kor, info = master[name][pjt]
                    results.append({"English_Name": name, "PJT": pjt, "Korean_Name": kor, "Business_Info": info})
                    found = True; break
        if not found:
            results.append({"English_Name": name, "PJT": "-", "Korean_Name": "미등록", "Business_Info": "정보 없음"})
    return pd.DataFrame(results)

# ---------------------------------------------------------
# [로직 고정] 전략적 PK 분석
# ---------------------------------------------------------
def analyze_strategic_pks(df):
    if df is None or df.empty: return pd.DataFrame(), 0
    total_rows = len(df)
    target_cols = [col for col in df.columns if df[col].notnull().any()]
    results = []
    for col in target_cols:
        nunique = df[col].nunique()
        if nunique == total_rows:
            results.append({"구분": "단일 PK", "컬럼 1": col, "컬럼 2": "-", "컬럼 3": "-", "유일성(%)": 100.0, "상태": "✅ 확정"})
    if results: return pd.DataFrame(results), total_rows
    top_potential = sorted(target_cols, key=lambda c: df[c].nunique(), reverse=True)[:10]
    for col in top_potential:
        results.append({"구분": "단일 PK", "컬럼 1": col, "컬럼 2": "-", "컬럼 3": "-", "유일성(%)": round((df[col].nunique()/total_rows)*100, 2), "상태": "⚠️ 후보"})
    return pd.DataFrame(results).head(10).reset_index(drop=True), total_rows

# ---------------------------------------------------------
# 2. 사이드바 (레이아웃 고정)
# ---------------------------------------------------------
st.sidebar.header("⚙️ 분석 설정")
w_name = st.sidebar.number_input("Name Similarity", 0.0, 1.0, 0.30, 0.05)
w_type = st.sidebar.number_input("Type Similarity", 0.0, 1.0, 0.10, 0.05)
w_value = st.sidebar.number_input("Value Overlap", 0.0, 1.0, 0.60, 0.05)
weights_tuple = (w_name, w_type, w_value)

st.sidebar.markdown("---")
rel_type_input = st.sidebar.radio("테이블 관계 설정", ["자동 탐지", "1:1", "1:N", "N:1"], index=0)

st.sidebar.markdown("---")
st.sidebar.subheader("📂 카탈로그 PJT 우선순위")
priority_list = ["LG D2C", "PJT_A", "PJT_B"]
pjt_1 = st.sidebar.selectbox("우선순위 1", priority_list, index=0)
pjt_2 = st.sidebar.selectbox("우선순위 2", ["None"] + priority_list, index=0)
priority_pjts = [p for p in [pjt_1, pjt_2] if p != "None"]

# ---------------------------------------------------------
# 3. 메인 레이아웃 (파일 업로드 및 분석)
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
        df_source_meta, _ = summarize_table_profile(profile_a)
        full_nunique_map_a = {col: profile_a.sample_df[col].nunique() for col in profile_a.sample_df.columns}
        row_count_a = len(profile_a.sample_df)
        
        df_pk_res_a, _ = analyze_strategic_pks(profile_a.sample_df)
        pk_a = df_pk_res_a.iloc[0]["컬럼 1"] if not df_pk_res_a.empty else "MODEL_CODE"

        st.success(f"전수 분석 완료 (유효 행 수: {row_count_a}) ✅")
        st.subheader("1. 소스 테이블 스키마 및 데이터 분석")
        c_main, c_side = st.columns([2, 1])
        with c_main:
            st.markdown("**📊 필드별 데이터 프로파일 요약**")
            df_source_meta["Distinct"] = df_source_meta["Column"].map(full_nunique_map_a).fillna(0).astype(int)
            st.dataframe(df_source_meta.sort_values(by="Distinct", ascending=False).style.apply(
                lambda r: ['background-color: #D1E9F6']*len(r) if r["Column"] == pk_a else ['']*len(r), axis=1
            ), use_container_width=True)
        with c_side:
            st.markdown("**📌 PK 후보 데이터 중복 상세 분석**")
            dupes = profile_a.sample_df[pk_a].value_counts()
            st.table(pd.DataFrame({"중복 데이터": dupes[dupes>1].index, "건수": dupes[dupes>1].values}).head(10))

        st.markdown("### 📊 상세 PK 분석 결과")
        st.table(df_pk_res_a)

        if file_b:
            profile_b = get_cached_profile(file_b.getvalue(), "TGT", sheet_b)
            all_matches = match_schemas(profile_a, profile_b, weights=weights_tuple)
            
            row_count_b = len(profile_b.sample_df)
            full_nunique_map_b = {col: profile_b.sample_df[col].nunique() for col in profile_b.sample_df.columns}
            df_pk_res_b, _ = analyze_strategic_pks(profile_b.sample_df)
            pk_b = df_pk_res_b.iloc[0]["컬럼 1"] if not df_pk_res_b.empty else "ITEM_ID"

            for m in all_matches:
                a_col, b_col = m['A_column'], m['B_column']
                is_pk_a = (full_nunique_map_a.get(a_col, 0) == row_count_a)
                is_pk_b = (full_nunique_map_b.get(b_col, 0) == row_count_b)
                
                # 점수 및 Overlap 재계산 (전수 분석 동기화)
                set_a = set(profile_a.sample_df[a_col].dropna().unique())
                set_b = set(profile_b.sample_df[b_col].dropna().unique())
                m['value_overlap'] = len(set_a.intersection(set_b)) / len(set_a) if set_a else 0.0
                m['score'] = (m['name_similarity'] * w_name) + (m['type_similarity'] * w_type) + (m['value_overlap'] * w_value)
                
                if is_pk_a and is_pk_b: m['relationship_detected'] = "1:1"
                elif is_pk_a and not is_pk_b: m['relationship_detected'] = "1:N"
                elif not is_pk_a and is_pk_b: m['relationship_detected'] = "N:1"
                else: m['relationship_detected'] = "N:M"
                
                m['is_valid_pk_match'] = (is_pk_a or is_pk_b) and (a_col == b_col)
                if m['score'] >= 0.9 or m['is_valid_pk_match']: m['match_type'] = 'strong'
                elif m['score'] >= 0.6: m['match_type'] = 'candidate'
                else: m['match_type'] = 'weak'

            # 정렬 및 필터링
            filtered_matches = [m for m in all_matches if m.get('match_type') != 'weak' or m.get('is_valid_pk_match')]
            type_priority = {'strong': 0, 'candidate': 1, 'weak': 2}
            filtered_matches = sorted(filtered_matches, key=lambda x: (type_priority.get(x['match_type'], 3), -x['score']))
            
            top_m = filtered_matches[0]
            rel_final = rel_type_input if rel_type_input != "자동 탐지" else top_m['relationship_detected']
            
            st.subheader(f"2. 소스 ↔ 타겟 매칭 추천 결과 (시스템이 탐지한 관계: {rel_final})")
            display_cols = ['A_column', 'B_column', 'name_similarity', 'type_similarity', 'value_overlap', 'score', 'relationship_detected', 'match_type']
            st.dataframe(pd.DataFrame(filtered_matches)[display_cols].reset_index(drop=True), use_container_width=True)

            st.subheader("3. JOIN KEY 후보 및 ERD 분석 결과")
            dot = Digraph(comment="ERD"); dot.attr(rankdir="LR")
            def make_node(t_name, pk_col, join_col):
                cols = []
                if pk_col: cols.append(('PK', pk_col))
                if join_col and join_col != pk_col: cols.append(('JOIN', join_col))
                rows = "".join([f'<TR><TD ALIGN="LEFT" BGCOLOR="{"#D1E9F6" if c[1]==join_col else "white"}">{"🔑 " if c[0]=="PK" else "   "}{c[1]}</TD></TR>' for c in cols])
                return f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0"><TR><TD BGCOLOR="#F4B2AF"><B>{t_name}</B></TD></TR>{rows}</TABLE>>'

            dot.node("A", label=make_node(profile_a.table_name, pk_a, top_m['A_column']), shape="plaintext")
            dot.node("B", label=make_node(profile_b.table_name, pk_b, top_m['B_column']), shape="plaintext")
            
            if rel_final == "N:1": dot.edge("A", "B", label="N:1", arrowhead="none", arrowtail="crow", dir="both")
            elif rel_final == "1:N": dot.edge("A", "B", label="1:N", arrowhead="crow", arrowtail="none", dir="both")
            else: dot.edge("A", "B", label=rel_final, dir="both")
            st.graphviz_chart(dot)

            st.markdown(f"**📜 비즈니스 로직 기반 SQL 쿼리 추천 (적용 관계: {rel_final})**")
            formatted_sql = f"""
SELECT 
    A.{pk_a if pk_a else '*'}, 
    B.{pk_b if pk_b else '*'}
FROM {profile_a.table_name} A
LEFT JOIN {profile_b.table_name} B 
    ON A.{top_m['A_column']} = B.{top_m['B_column']}
WHERE A.{top_m['A_column']} IS NOT NULL;
            """
            st.code(formatted_sql, language="sql")

            # ---------------------------------------------------------
            # 4. [요청 반영] 시인성이 강화된 상세 분석 리포트 (최하단)
            # ---------------------------------------------------------
            st.markdown("---")
            st.subheader("📂 카탈로그 매핑 및 비즈니스 상세 분석 리포트")
            
            df_tbl_rpt = lookup_catalog_report([profile_a.table_name, profile_b.table_name], priority_pjts, type='table')
            df_col_rpt = lookup_catalog_report(list(set([pk_a, pk_b, top_m['A_column'], top_m['B_column']])), priority_pjts, type='column')
            
            src_cat = df_tbl_rpt[df_tbl_rpt['English_Name']==profile_a.table_name].iloc[0]
            tgt_cat = df_tbl_rpt[df_tbl_rpt['English_Name']==profile_b.table_name].iloc[0]
            key_cat = df_col_rpt[df_col_rpt['English_Name']==top_m['A_column']].iloc[0]
            
            # [요청 반영] HTML 태그를 활용하여 특정 단어만 글자 크기 + 볼딩 처리
            st.markdown(f"""
            <div style="background-color: #e8f4f9; padding: 20px; border-radius: 10px; border-left: 5px solid #2196F3;">
                <h4 style="margin-top: 0; color: #0c5460;">📝 데이터 아키텍처 분석 요약</h4>
                <ul style="margin-bottom: 0; line-height: 1.8;">
                    <li><b>업무 관계</b>: 소스 테이블 <span style="font-size:1.15em;"><b>{src_cat['Korean_Name']}</b></span>와(과) 타겟 테이블 <span style="font-size:1.15em;"><b>{tgt_cat['Korean_Name']}</b></span>는(은) <span style="font-size:1.25em; color: #d63384;"><b>{rel_final}</b></span> 관계를 맺고 있습니다.</li>
                    <li><b>데이터 흐름</b>: <code>{src_cat['Business_Info']}</code> 데이터가 <code>{tgt_cat['Business_Info']}</code> 기준 정보를 참조하여 관리되는 구조입니다.</li>
                    <li><b>핵심 조인 키</b>: 비즈니스 공통 키인 <span style="font-size:1.15em;"><b>{key_cat['English_Name']}</b></span>(<span style="font-size:1.1em;"><b>{key_cat['Korean_Name']}</b></span>)를 매개체로 조인됩니다. ({key_cat['Business_Info']})</li>
                    <li><b>분석 가이드</b>: 조인 시 <span style="font-size:1.2em;"><b>{rel_final}</b></span> 구조에 따라 데이터 정합성을 확인해야 하며, 특히 <span style="font-size:1.15em;"><b>{key_cat['Korean_Name']}</b></span>의 미매핑 데이터 발생 여부를 모니터링하는 것이 권장됩니다.</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
            
            st.write("") # 간격 조절
            rpt_col1, rpt_col2 = st.columns(2)
            with rpt_col1:
                st.markdown("##### 📋 테이블 카탈로그 (`tbl_ctlg_m`) 상세")
                st.table(df_tbl_rpt)
            with rpt_col2:
                st.markdown("##### 📑 컬럼 카탈로그 (`col_ctlg_m`) 상세")
                st.dataframe(df_col_rpt, use_container_width=True)

    except Exception as e:
        st.error(f"오류: {e}")