import streamlit as st
import streamlit.components.v1 as components
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
# [데이터 정제 및 캐싱] 
# ---------------------------------------------------------
def clean_df(df):
    if df is None: return None
    df = df.dropna(how='all').copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.upper()
        df[col] = df[col].replace(['NAN', 'NONE', 'NAT', '<NA>'], pd.NA)
    return df

@st.cache_data(show_spinner=False)
def get_cached_profile(file_bytes, table_id, sheet_name):
    profile = read_excel_to_profile(file_bytes, table_id)
    full_df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)
    profile.sample_df = clean_df(full_df)
    return profile

# ---------------------------------------------------------
# 🔌 Neon DB 실시간 연결 및 스키마 원본 데이터 로드
# ---------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner=False) 
def load_catalog_from_neon():
    try:
        db_url = "postgresql://neondb_owner:npg_p1S5sHjFXYfm@ep-fancy-queen-a15e3itu-pooler.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=require"
        conn = st.connection("neon_db", type="sql", url=db_url)
        
        tbl_df = conn.query('SELECT * FROM tbl_ctlg_m')
        col_df = conn.query('SELECT * FROM col_ctlg_m')
        
        tbl_df.columns = [c.lower() for c in tbl_df.columns]
        col_df.columns = [c.lower() for c in col_df.columns]
        
        if 'tbl_kor_desc' in tbl_df.columns and 'tbl_kor_name' in tbl_df.columns:
            tbl_df['tbl_kor_desc'] = tbl_df['tbl_kor_desc'].replace(r'^\s*$', pd.NA, regex=True).fillna(tbl_df['tbl_kor_name'])
            
        if 'col_kor_desc' in col_df.columns and 'col_kor_name' in col_df.columns:
            col_df['col_kor_desc'] = col_df['col_kor_desc'].replace(r'^\s*$', pd.NA, regex=True).fillna(col_df['col_kor_name'])
        
        return tbl_df, col_df
    except Exception as e:
        st.sidebar.error(f"⚠️ Neon DB 연결 실패: {e}")
        return pd.DataFrame(), pd.DataFrame()

tbl_ctlg_m, col_ctlg_m = load_catalog_from_neon()

def get_actual_pjts_from_catalog():
    if tbl_ctlg_m.empty and col_ctlg_m.empty:
        return ["DB 연결 필요"]
    pjt_tbl = tbl_ctlg_m['pjt'].dropna().unique().tolist() if 'pjt' in tbl_ctlg_m.columns else []
    pjt_col = col_ctlg_m['pjt'].dropna().unique().tolist() if 'pjt' in col_ctlg_m.columns else []
    pjts = sorted(list(set(pjt_tbl + pjt_col)))
    return pjts if pjts else ["PJT 없음"]

def lookup_catalog_report(names, pjts, type='table'):
    results = []
    df_target = tbl_ctlg_m if type == 'table' else col_ctlg_m
    name_col = 'tbl_name' if type == 'table' else 'col_name'
    if df_target.empty or name_col not in df_target.columns or 'pjt' not in df_target.columns:
        for name in names:
            results.append({name_col: name, "pjt": "-", "info": "DB 테이블 또는 컬럼 구조 에러"})
        return pd.DataFrame(results)
    for name in names:
        found = False
        match_df = df_target[df_target[name_col] == name]
        if not match_df.empty:
            for pjt in pjts:
                pjt_match = match_df[match_df['pjt'] == pjt]
                if not pjt_match.empty:
                    results.append(pjt_match.iloc[0].to_dict())
                    found = True; break
        if not found:
            default_row = {name_col: name, "pjt": "-"}
            if type == 'table': default_row.update({"tbl_kor_name": "미등록", "tbl_kor_desc": "카탈로그 정보 없음"})
            else: default_row.update({"col_kor_name": "미등록", "col_kor_desc": "카탈로그 정보 없음"})
            results.append(default_row)
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
st.sidebar.markdown("""<style>div[data-baseweb="select"] input {caret-color: transparent !important;}</style>""", unsafe_allow_html=True)
st.sidebar.header("⚙️ 분석 설정")
w_name = st.sidebar.number_input("Name Similarity", 0.0, 1.0, 0.30, 0.05)
w_type = st.sidebar.number_input("Type Similarity", 0.0, 1.0, 0.10, 0.05)
w_value = st.sidebar.number_input("Value Overlap", 0.0, 1.0, 0.60, 0.05)
weights_tuple = (w_name, w_type, w_value)
st.sidebar.markdown("---")

# UI에서 제거된 라디오 버튼 (에러 방지를 위해 변수만 기본값으로 내부 선언)
rel_type_input = "자동 탐지" 

st.sidebar.subheader("📂 카탈로그 PJT 우선순위")
catalog_pjt_list = get_actual_pjts_from_catalog()
pjt_1 = st.sidebar.selectbox("우선순위 1", catalog_pjt_list, index=0)
idx_2 = 1 if len(catalog_pjt_list) > 1 else 0
pjt_2 = st.sidebar.selectbox("우선순위 2", catalog_pjt_list, index=idx_2)
priority_pjts = [pjt_1, pjt_2]

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
        pk_a = df_pk_res_a.iloc[0]["컬럼 1"] if not df_pk_res_a.empty else None
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
            if pk_a:
                dupes = profile_a.sample_df[pk_a].value_counts()
                st.table(pd.DataFrame({"중복 데이터": dupes[dupes>1].index, "건수": dupes[dupes>1].values}).head(10))
        
        st.markdown("### 📊 상세 PK 분석 결과")
        st.table(df_pk_res_a)
        
        if not df_pk_res_a.empty:
            confirmed_pks = df_pk_res_a[df_pk_res_a["상태"] == "✅ 확정"]["컬럼 1"].tolist()
            if confirmed_pks:
                st.markdown("##### 🔑 확정 PK 컬럼 카탈로그 정보")
                df_confirmed_cat = lookup_catalog_report(confirmed_pks, priority_pjts, type='column')
                st.table(df_confirmed_cat)

        if file_b:
            profile_b = get_cached_profile(file_b.getvalue(), "TGT", sheet_b)
            row_count_b = len(profile_b.sample_df)
            df_pk_res_b, _ = analyze_strategic_pks(profile_b.sample_df)
            pk_b = df_pk_res_b.iloc[0]["컬럼 1"] if not df_pk_res_b.empty else None

            file_sig = f"{file_a.name}_{file_a.size}_{sheet_a}_{file_b.name}_{file_b.size}_{sheet_b}_{w_name}_{w_type}_{w_value}"
            if st.session_state.get("last_run_sig") != file_sig:
                with st.spinner("🔍 타겟 데이터 전수 분석 및 조인 키 매칭을 진행 중입니다..."):
                    all_matches_raw = match_schemas(profile_a, profile_b, weights=weights_tuple)
                    full_nunique_map_b = {col: profile_b.sample_df[col].nunique() for col in profile_b.sample_df.columns}
                    existing_pairs = {(m['A_column'], m['B_column']) for m in all_matches_raw}
                    common_cols = set(profile_a.sample_df.columns).intersection(set(profile_b.sample_df.columns))
                    for col in common_cols:
                        if (col, col) not in existing_pairs:
                            all_matches_raw.append({'A_column': col, 'B_column': col, 'name_similarity': 1.0, 'type_similarity': 1.0})
                    seen_pairs = set(); final_list = []
                    for m in all_matches_raw:
                        a_col, b_col = m['A_column'], m['B_column']
                        if (a_col, b_col) in seen_pairs: continue
                        seen_pairs.add((a_col, b_col))
                        if a_col in profile_a.sample_df.columns and b_col in profile_b.sample_df.columns:
                            set_a = set(profile_a.sample_df[a_col].dropna().unique()); set_b = set(profile_b.sample_df[b_col].dropna().unique())
                            m['value_overlap'] = len(set_a.intersection(set_b)) / len(set_a) if set_a else 0.0
                        else: m['value_overlap'] = 0.0
                        m['score'] = (m.get('name_similarity', 0) * w_name) + (m.get('type_similarity', 0) * w_type) + (m['value_overlap'] * w_value)
                        is_pk_a = (full_nunique_map_a.get(a_col, 0) == row_count_a); is_pk_b = (full_nunique_map_b.get(b_col, 0) == row_count_b)
                        if is_pk_a and is_pk_b: m['relationship_detected'] = "1:1"
                        elif is_pk_a and not is_pk_b: m['relationship_detected'] = "1:N"
                        elif not is_pk_a and is_pk_b: m['relationship_detected'] = "N:1"
                        else: m['relationship_detected'] = "N:M"
                        m['is_valid_pk_match'] = (is_pk_a or is_pk_b) and (a_col == b_col)
                        if m['score'] >= 0.9 or m['is_valid_pk_match']: m['match_type'] = 'strong'
                        elif m['score'] >= 0.6: m['match_type'] = 'candidate'
                        else: m['match_type'] = 'weak'
                        final_list.append(m)
                    all_matches_sorted = sorted(final_list, key=lambda x: ({'strong': 0, 'candidate': 1, 'weak': 2}.get(x['match_type'], 3), -x['score']))
                    st.session_state["match_results"] = all_matches_sorted
                    st.session_state["last_run_sig"] = file_sig
                    if "applied_keys" in st.session_state: del st.session_state["applied_keys"]

            all_matches_sorted = st.session_state["match_results"]
            
            if rel_type_input != "자동 탐지":
                all_matches_sorted = [m for m in all_matches_sorted if m['relationship_detected'] == rel_type_input]
                header_text = f"사용자가 정의한 관계: {rel_type_input}"
            else:
                top_m = all_matches_sorted[0] if all_matches_sorted else {}
                header_text = f"시스템이 탐지한 관계: {top_m.get('relationship_detected', 'N:M')}"

            st.subheader(f"2. 소스 ↔ 타겟 매칭 결과 선택 ({header_text})")

            if not all_matches_sorted and rel_type_input != "자동 탐지":
                st.warning(f"⚠️ 현재 데이터 프로파일 분석 결과, 두 테이블 간에 '{rel_type_input}' 관계를 만족하는 조인 키(PK/FK 등) 조합이 존재하지 않습니다.")
            else:
                sel_types = st.multiselect("Match Type 필터", ["strong", "candidate", "weak"], default=["strong", "candidate"])
                df_display = pd.DataFrame(all_matches_sorted)
                df_display = df_display[df_display['match_type'].isin(sel_types)].copy().reset_index(drop=True)
                df_display.insert(0, 'Select', False)
                
                if "applied_keys" in st.session_state:
                    if not st.session_state['applied_keys'].empty and not df_display.empty:
                        applied_pairs = set(zip(st.session_state['applied_keys']['A_column'], st.session_state['applied_keys']['B_column']))
                        mask = df_display.apply(lambda r: (r['A_column'], r['B_column']) in applied_pairs, axis=1)
                        df_display.loc[mask, 'Select'] = True
                else:
                    if not df_display.empty:
                        df_display.loc[0, 'Select'] = True; st.session_state['applied_keys'] = df_display[df_display['Select'] == True]

                edited_df = st.data_editor(
                    df_display[['Select', 'A_column', 'B_column', 'name_similarity', 'type_similarity', 'value_overlap', 'score', 'relationship_detected', 'match_type']],
                    column_config={"Select": st.column_config.CheckboxColumn("선택", default=False)},
                    disabled=['A_column', 'B_column', 'name_similarity', 'type_similarity', 'value_overlap', 'score', 'relationship_detected', 'match_type'],
                    hide_index=True, use_container_width=True, key="join_selector"
                )

                if st.button("🚀 선택한 조인 키 적용 (ERD 및 SQL 업데이트)"):
                    st.session_state['applied_keys'] = edited_df[edited_df['Select'] == True]
                    st.rerun()

                selected_keys = st.session_state.get('applied_keys', pd.DataFrame())

                if not selected_keys.empty:
                    rel_rank = {"N:M": 3, "N:1": 2, "1:N": 2, "1:1": 1}
                    sorted_rels = sorted(selected_keys['relationship_detected'].tolist(), key=lambda x: rel_rank.get(x, 0), reverse=True)
                    rel_final = rel_type_input if rel_type_input != "자동 탐지" else sorted_rels[0]

                    st.subheader(f"3. JOIN KEY 후보 및 ERD 분석 결과 (적용 관계: {rel_final})")
                    dot = Digraph(comment="ERD"); dot.attr(rankdir="LR")
                    def make_node(t_name, pk, join_cols):
                        rows = [f'<TR><TD BGCOLOR="#F4B2AF"><B>{t_name}</B></TD></TR>']
                        if pk: rows.append(f'<TR><TD ALIGN="LEFT">🔑 {pk}</TD></TR>')
                        for c in join_cols:
                            if c != pk: rows.append(f'<TR><TD ALIGN="LEFT" BGCOLOR="#D1E9F6">   {c}</TD></TR>')
                        return f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">{"".join(rows)}</TABLE>>'
                    dot.node("A", label=make_node(profile_a.table_name, pk_a, selected_keys['A_column'].tolist()), shape="plaintext")
                    dot.node("B", label=make_node(profile_b.table_name, pk_b, selected_keys['B_column'].tolist()), shape="plaintext")
                    if rel_final == "N:1": dot.edge("A", "B", label="N:1", arrowhead="none", arrowtail="crow", dir="both")
                    elif rel_final == "1:N": dot.edge("A", "B", label="1:N", arrowhead="crow", arrowtail="none", dir="both")
                    elif rel_final == "N:M": dot.edge("A", "B", label="N:M", arrowhead="crow", arrowtail="crow", dir="both")
                    else: dot.edge("A", "B", label=rel_final, dir="both")
                    st.graphviz_chart(dot)

                    sql_join_type = "LEFT JOIN"
                    if rel_final == "1:1":
                        sql_join_type = "INNER JOIN"
                    elif rel_final == "1:N" or rel_final == "N:1":
                        sql_join_type = "LEFT JOIN"
                    elif rel_final == "N:M":
                        sql_join_type = "INNER JOIN"

                    where_target_pk = pk_b if pk_b else selected_keys.iloc[0]['B_column']
                    join_conds = "\n    AND ".join([f"A.{r['A_column']} = B.{r['B_column']}" for _, r in selected_keys.iterrows()])
                    
                    formatted_sql = f"""SELECT \n    A.*, \n    B.* \nFROM {profile_a.table_name} A \n{sql_join_type} {profile_b.table_name} B \nON {join_conds}\nWHERE 1=1\nAND B.{where_target_pk} IS NOT NULL;"""
                    
                    st.markdown(f"**📜 비즈니스 로직 기반 SQL 쿼리 추천**")
                    st.code(formatted_sql, language="sql")
                    
                    copy_html = f"""
                    <div style="margin-top: 5px;">
                        <textarea id="sql_code" style="position: absolute; left: -9999px;">{formatted_sql}</textarea>
                        <button onclick="copyToClipboard()" style="background-color: #ffffff; border: 1px solid #d1d5db; color: #374151; padding: 6px 14px; cursor: pointer; border-radius: 6px; font-weight: 500;">📋 SQL 구문 복사하기</button>
                        <script>
                        function copyToClipboard() {{
                            var copyText = document.getElementById("sql_code");
                            copyText.select(); document.execCommand("copy");
                            var btn = document.querySelector("button");
                            btn.innerHTML = "✅ 복사 완료!";
                            setTimeout(function() {{ btn.innerHTML = "📋 SQL 구문 복사하기"; }}, 2000);
                        }}
                        </script>
                    </div>"""
                    components.html(copy_html, height=50)

                    st.markdown("---")
                    st.subheader("📂 카탈로그 매핑 및 비즈니스 상세 분석 리포트")
                    df_tbl_rpt = lookup_catalog_report([profile_a.table_name, profile_b.table_name], priority_pjts, type='table')
                    catalog_cols = list(set([c for c in [pk_a, pk_b] + selected_keys['A_column'].tolist() + selected_keys['B_column'].tolist() if c]))
                    df_col_rpt = lookup_catalog_report(catalog_cols, priority_pjts, type='column')
                    
                    src_cat = df_tbl_rpt[df_tbl_rpt['tbl_name']==profile_a.table_name].iloc[0].to_dict() if not df_tbl_rpt.empty and 'tbl_name' in df_tbl_rpt.columns and len(df_tbl_rpt[df_tbl_rpt['tbl_name']==profile_a.table_name]) > 0 else {}
                    tgt_cat = df_tbl_rpt[df_tbl_rpt['tbl_name']==profile_b.table_name].iloc[0].to_dict() if not df_tbl_rpt.empty and 'tbl_name' in df_tbl_rpt.columns and len(df_tbl_rpt[df_tbl_rpt['tbl_name']==profile_b.table_name]) > 0 else {}
                    
                    src_cat_name = src_cat.get('tbl_kor_name', profile_a.table_name)
                    tgt_cat_name = tgt_cat.get('tbl_kor_name', profile_b.table_name)
                    src_cat_desc = src_cat.get('tbl_kor_desc', '정보 없음')
                    tgt_cat_desc = tgt_cat.get('tbl_kor_desc', '정보 없음')

                    key_names_html = " + ".join([f"<b>{k}</b>" for k in selected_keys['A_column'].tolist()])
                    st.markdown(f"""
                    <div style="background-color: #e8f4f9; padding: 22px; border-radius: 12px; border-left: 6px solid #2196F3;">
                        <h4 style="margin-top: 0; color: #0c5460;">📝 데이터 아키텍처 분석 요약</h4>
                        <ul style="line-height: 2.0;">
                            <li><b>업무 관계</b>: 소스 테이블 <b>{src_cat_name}</b>와(과) 타겟 테이블 <b>{tgt_cat_name}</b>는(은) <span style="color: #d63384;"><b>{rel_final}</b></span> 관계로 연결됩니다.</li>
                            <li><b>핵심 조인 키</b>: 현재 {key_names_html} 조합을 통해 조인 분석이 수행되고 있습니다.</li>
                            <li><b>비즈니스 흐름</b>: <code>{src_cat_desc}</code> 실적 데이터를 <code>{tgt_cat_desc}</code> 기준 정보를 통해 심화 분석할 수 있는 구조입니다.</li>
                        </ul>
                    </div>""", unsafe_allow_html=True)
                    st.write("")
                    st.markdown("##### 📋 테이블 카탈로그 상세")
                    st.table(df_tbl_rpt)
                    st.markdown("##### 📑 컬럼 카탈로그 상세")
                    st.dataframe(df_col_rpt, use_container_width=True)
                else: st.warning("분석할 Join Key의 체크박스를 선택한 후 '적용' 버튼을 눌러주세요.")
    except Exception as e: st.error(f"오류: {e}")