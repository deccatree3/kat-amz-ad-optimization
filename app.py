import os
import streamlit as st
import pandas as pd
import json
import re
import sqlite3
import datetime
from io import BytesIO

st.set_page_config(page_title="Amazon JP 광고 최적화", layout="wide", page_icon="🎯")

# ── 상수 ──────────────────────────────────────────────────────────────────────
COST_THRESHOLD = 2000   # JPY
ACOS_THRESHOLD = 50.0   # %

BRAND_KEYWORDS = [
    "リードルショット", " リードルショット", "りーどるしょっと", "にーどるしょっと",
    "vtリードルショット", "vt リードルショット",
    "マイクロニードル", "ニードル",
]

JUDGMENT_OPTIONS = [
    "✅ 제외",
    "⬜ 유지(중요키워드 ACOS≤80% — 제외 예외)",
    "⬜ 유지(경쟁사ASIN ≤80% — 제외 예외)",
    "🔗 확인 필요 (ASIN)",
]

REPO_DIR          = os.path.dirname(os.path.abspath(__file__))
WRITE_DIR         = REPO_DIR if os.name == "nt" else "/tmp"  # 클라우드는 /tmp만 쓰기 가능
TRANSLATIONS_FILE = os.path.join(REPO_DIR,  "translations.json")
OVERRIDES_FILE    = os.path.join(WRITE_DIR, "overrides.json")
DB_FILE           = os.path.join(WRITE_DIR, "history.db")


# ── 유틸 ──────────────────────────────────────────────────────────────────────
def parse_pct(val) -> float:
    try:
        s = str(val).replace(",", "").strip()
        if "%" in s:
            return float(s.replace("%", "").strip())
        else:
            v = float(s)
            if 0 < v <= 1:
                return v * 100
            return v
    except Exception:
        return 0.0


def is_asin(text: str) -> bool:
    return bool(re.match(r'^[Bb][0-9A-Za-z]{9}$', str(text).strip()))


# ── 오버라이드 저장/로드 ───────────────────────────────────────────────────────
def load_overrides() -> dict:
    if "overrides" not in st.session_state:
        try:
            with open(OVERRIDES_FILE, encoding="utf-8") as f:
                st.session_state.overrides = json.load(f)
        except Exception:
            st.session_state.overrides = {}
    return st.session_state.overrides


def save_overrides(overrides: dict):
    st.session_state.overrides = overrides
    try:
        with open(OVERRIDES_FILE, "w", encoding="utf-8") as f:
            json.dump(overrides, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"저장 실패: {e}")


# ── DB ───────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            날짜      TEXT,
            캠페인    TEXT,
            검색어    TEXT,
            발음      TEXT,
            의미      TEXT,
            매치타입  TEXT,
            비용      INTEGER,
            구매수    INTEGER,
            매출      INTEGER,
            ACOS      TEXT,
            제외타입  TEXT,
            판단      TEXT
        )
    """)
    conn.commit()
    return conn


def save_to_db(df: pd.DataFrame, date_str: str):
    cols = ["캠페인", "검색어", "발음", "의미", "매치타입", "비용", "구매수", "매출", "ACOS", "제외타입", "판단"]
    snapshot = df[cols].copy()
    snapshot.insert(0, "날짜", date_str)
    with get_db() as conn:
        snapshot.to_sql("history", conn, if_exists="append", index=False)


def query_history(date_from: str, date_to: str, campaigns: list, judgments: list) -> pd.DataFrame:
    with get_db() as conn:
        placeholders_c = ",".join("?" * len(campaigns))  if campaigns  else "''"
        placeholders_j = ",".join("?" * len(judgments))  if judgments  else "''"
        params = [date_from, date_to] + (campaigns or []) + (judgments or [])
        sql = f"""
            SELECT * FROM history
            WHERE 날짜 BETWEEN ? AND ?
            {"AND 캠페인 IN (" + placeholders_c + ")" if campaigns else ""}
            {"AND 판단   IN (" + placeholders_j + ")" if judgments else ""}
            ORDER BY 날짜 DESC, 비용 DESC
        """
        return pd.read_sql(sql, conn, params=params)


def get_history_meta() -> dict:
    """DB에서 날짜·캠페인 목록 조회"""
    try:
        with get_db() as conn:
            dates = pd.read_sql("SELECT DISTINCT 날짜 FROM history ORDER BY 날짜 DESC", conn)["날짜"].tolist()
            camps = pd.read_sql("SELECT DISTINCT 캠페인 FROM history ORDER BY 캠페인", conn)["캠페인"].tolist()
        return {"dates": dates, "campaigns": camps}
    except Exception:
        return {"dates": [], "campaigns": []}


# ── CSV 로드 ──────────────────────────────────────────────────────────────────
@st.cache_data
def load_csv(file_bytes: bytes, campaign_name: str) -> pd.DataFrame:
    df = None
    for enc in ("utf-8-sig", "utf-8", "shift-jis", "cp932"):
        try:
            df = pd.read_csv(BytesIO(file_bytes), encoding=enc)
            break
        except Exception:
            continue
    if df is None:
        st.error(f"[{campaign_name}] CSV 파일을 읽을 수 없습니다. 인코딩을 확인해주세요.")
        return pd.DataFrame()

    col_map = {}
    for col in df.columns:
        c = col.strip()
        if "검색어" in c or "query" in c.lower():
            col_map[col] = "검색어"
        elif "키워드" in c or "keyword" in c.lower() or "match" in c.lower():
            col_map[col] = "매치타입"
        elif "비용" in c or "cost" in c.lower() or "spend" in c.lower():
            col_map[col] = "비용"
        elif "구매" in c or "purchase" in c.lower() or "order" in c.lower():
            col_map[col] = "구매수"
        elif "매출" in c or "sales" in c.lower() or "revenue" in c.lower():
            col_map[col] = "매출"
        elif "acos" in c.lower():
            col_map[col] = "ACOS"
        elif "roas" in c.lower():
            col_map[col] = "ROAS"

    df = df.rename(columns=col_map)

    needed = ["검색어", "매치타입", "비용", "구매수", "매출", "ACOS", "ROAS"]
    for col in needed:
        if col not in df.columns:
            df[col] = None

    if "매치타입" in df.columns:
        df["매치타입"] = df["매치타입"].astype(str).str.strip()

    df = df[df["검색어"].notna() & (df["검색어"].astype(str).str.strip() != "")].copy()

    df["비용"]  = pd.to_numeric(df["비용"],  errors="coerce").fillna(0).round(0).astype(int)
    df["구매수"] = pd.to_numeric(df["구매수"], errors="coerce").fillna(0).astype(int)
    df["매출"]  = pd.to_numeric(df["매출"],  errors="coerce").fillna(0).round(0).astype(int)

    if df["ACOS"].isna().all():
        df["ACOS_num"] = df.apply(
            lambda r: (r["비용"] / r["매출"] * 100) if r["매출"] > 0 else 999.0, axis=1
        )
        df["ACOS"] = df["ACOS_num"].apply(lambda v: f"{v:.0f}%" if v < 999 else "999%")
    else:
        df["ACOS_num"] = df["ACOS"].apply(parse_pct)

    df["ROAS_num"] = df.apply(
        lambda r: (r["매출"] / r["비용"]) if r["비용"] > 0 else 0.0, axis=1
    )
    df["캠페인"] = campaign_name
    return df


# ── 번역 ──────────────────────────────────────────────────────────────────────
def load_translations() -> dict:
    if "translations" not in st.session_state:
        try:
            with open(TRANSLATIONS_FILE, encoding="utf-8") as f:
                st.session_state.translations = json.load(f)
        except Exception:
            st.session_state.translations = {}
    return st.session_state.translations


def attach_translations(df: pd.DataFrame) -> pd.DataFrame:
    cache = st.session_state.get("translations", {})
    df = df.copy()
    df["발음"] = df["검색어"].map(lambda k: cache.get(k, {}).get("발음", ""))
    df["의미"] = df["검색어"].map(lambda k: cache.get(k, {}).get("의미", ""))
    return df


# ── 제외 키워드 계산 ───────────────────────────────────────────────────────────
def calc_exclusions(df: pd.DataFrame, overrides: dict) -> pd.DataFrame:
    targets = df[df["매치타입"].isin(["loose-match", "substitutes"])].copy()
    cands = targets[
        (targets["비용"] >= COST_THRESHOLD) &
        (targets["ACOS_num"] > ACOS_THRESHOLD)
    ].copy()

    cands["제외타입"] = cands["매치타입"].map({
        "substitutes": "정확히 일치",
        "loose-match": "구문 일치",
    })

    def flag(row):
        kw = str(row["검색어"])
        # 사용자 오버라이드 우선 적용
        if kw in overrides:
            return overrides[kw]
        # ASIN이고 ACOS 50~80% → 확인 필요
        if is_asin(kw) and row["ACOS_num"] <= 80:
            return "🔗 확인 필요 (ASIN)"
        # 중요 키워드이고 ACOS ≤ 80% → 유지
        if any(bkw in kw for bkw in BRAND_KEYWORDS) and row["ACOS_num"] <= 80:
            return "⬜ 유지(중요키워드 ACOS≤80% — 제외 예외)"
        return "✅ 제외"

    cands["판단"] = cands.apply(flag, axis=1)

    # 확인 필요(ASIN) 행에만 상품 링크 추가
    cands["상품링크"] = cands.apply(
        lambda r: f"https://www.amazon.co.jp/dp/{r['검색어'].upper()}"
        if r["판단"] == "🔗 확인 필요 (ASIN)" else "",
        axis=1,
    )

    return cands.sort_values("비용", ascending=False).reset_index(drop=True)


# ── 엑셀 내보내기 ─────────────────────────────────────────────────────────────
def to_excel(confirmed: pd.DataFrame, kept: pd.DataFrame) -> bytes:
    output = BytesIO()
    cols = ["캠페인", "검색어", "매치타입", "비용", "구매수", "매출", "ACOS", "제외타입", "판단"]
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        confirmed[cols].to_excel(writer, sheet_name="즉시 제외", index=False)
        if not kept.empty:
            kept[cols].to_excel(writer, sheet_name="유지(예외 처리)", index=False)
    return output.getvalue()


# ── 세션 초기화 ───────────────────────────────────────────────────────────────
if "campaigns" not in st.session_state:
    st.session_state.campaigns = {}


# ── 헤더 ──────────────────────────────────────────────────────────────────────
st.title("🎯 Amazon JP 광고 최적화 — 제외 키워드")
st.caption(
    "기준: 총비용 ≥ 2,000엔  AND  ACOS > 50%  |  "
    "loose-match → 구문 일치 제외 / substitutes → 정확히 일치 제외"
)
st.divider()

tab1, tab2, tab3 = st.tabs(["📥 데이터 업로드", "🔍 제외 키워드 검토", "📋 최종 목록 · 내보내기"])


# ══════════════════════════════════════════════════════════════════════════════
# Tab 1 — 데이터 업로드
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("캠페인 CSV 업로드")
    st.info(
        "아마존 광고센터 → 캠페인 → 광고그룹 → **검색어** 메뉴 → "
        "총비용 내림차순 → 300개 내보내기 한 CSV를 올려주세요.  \n"
        "여러 캠페인을 한 번에 선택할 수 있습니다. 파일명이 캠페인명으로 자동 설정됩니다."
    )

    if st.button("🗑 캐시 초기화"):
        load_csv.clear()
        st.session_state.campaigns = {}
        st.session_state.translations = {}
        save_overrides({})
        st.rerun()

    st.divider()

    uploaded_files = st.file_uploader(
        "또는 CSV 파일 직접 업로드 (여러 개 동시 선택 가능)",
        type="csv",
        accept_multiple_files=True,
        label_visibility="collapsed",
    )

    if uploaded_files:
        new_camps = [f.name.replace(".csv", "").strip() for f in uploaded_files]
        # 기존에 없던 캠페인이 새로 추가되면 override 초기화
        if any(c not in st.session_state.campaigns for c in new_camps):
            save_overrides({})
        for f in uploaded_files:
            camp_name = f.name.replace(".csv", "").strip()
            if camp_name not in st.session_state.campaigns:
                df_new = load_csv(f.read(), camp_name)
                st.session_state.campaigns[camp_name] = df_new

    if st.session_state.campaigns:
        st.divider()
        st.subheader("업로드된 캠페인")
        for name, df in list(st.session_state.campaigns.items()):
            c1, c2, c3, c4, c5 = st.columns([3, 1, 1, 1, 1])
            c1.write(f"**{name}**")
            c2.metric("검색어", f"{len(df):,}개")
            c3.metric("총비용", f"¥{df['비용'].sum():,.0f}")
            c4.metric("구매수", f"{df['구매수'].sum():,}건")
            if c5.button("🗑 삭제", key=f"del_{name}"):
                del st.session_state.campaigns[name]
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# Tab 2 — 제외 키워드 검토
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    if not st.session_state.campaigns:
        st.info("먼저 [데이터 업로드] 탭에서 CSV를 추가해주세요.")
    else:
        overrides = load_overrides()
        all_df    = pd.concat(st.session_state.campaigns.values(), ignore_index=True)

        with st.expander("🔧 디버그"):
            for camp, cdf in st.session_state.campaigns.items():
                st.markdown(f"**{camp}**")
                loose = len(cdf[cdf["매치타입"] == "loose-match"])
                subs  = len(cdf[cdf["매치타입"] == "substitutes"])
                hit   = len(cdf[(cdf["비용"] >= 2000) & (cdf["ACOS_num"] > 50)])
                st.write(f"ACOS 원본 샘플: {cdf['ACOS'].head(5).tolist()}")
                st.write(f"ACOS_num 샘플: {cdf['ACOS_num'].head(5).tolist()}")
                st.write(f"loose-match: {loose}개 / substitutes: {subs}개 / 조건 충족: {hit}개")
                st.dataframe(cdf[["검색어","매치타입","비용","ACOS","ACOS_num"]].head(5))
                st.divider()

        load_translations()
        cands = calc_exclusions(all_df, overrides)
        cands = attach_translations(cands)

        confirmed = cands[cands["판단"] == "✅ 제외"]
        pending   = cands[cands["판단"] == "🔗 확인 필요 (ASIN)"]

        # ── 요약 지표 ────────────────────────────────────────────────────────
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("예상 절감액",    f"¥{confirmed['비용'].sum():,.0f}")
        m2.metric("총 제외 후보",   f"{len(confirmed)}개")
        m3.metric("정확히 일치",    f"{len(confirmed[confirmed['제외타입']=='정확히 일치'])}개")
        m4.metric("구문 일치",      f"{len(confirmed[confirmed['제외타입']=='구문 일치'])}개")
        m5.metric("ASIN 확인 필요", f"{len(pending)}개")

        st.divider()

        # ── 필터 ─────────────────────────────────────────────────────────────
        f1, f2, f3 = st.columns(3)
        sel_camp   = f1.multiselect("캠페인", list(st.session_state.campaigns.keys()),
                                    default=list(st.session_state.campaigns.keys()), key="tab2_camp")
        sel_type   = f2.multiselect("제외타입", ["정확히 일치", "구문 일치"],
                                    default=["정확히 일치", "구문 일치"], key="tab2_type")
        sel_status = f3.multiselect("판단", JUDGMENT_OPTIONS, default=JUDGMENT_OPTIONS, key="tab2_status")

        view = cands[
            cands["캠페인"].isin(sel_camp) &
            cands["제외타입"].isin(sel_type) &
            cands["판단"].isin(sel_status)
        ].copy().reset_index(drop=True)

        # ── 편집 가능한 테이블 ────────────────────────────────────────────────
        disp_cols = ["캠페인", "검색어", "발음", "의미", "매치타입", "비용", "구매수", "매출", "ACOS", "판단", "상품링크"]

        edited = st.data_editor(
            view[disp_cols],
            column_config={
                "판단": st.column_config.SelectboxColumn(
                    "판단",
                    options=JUDGMENT_OPTIONS,
                    required=True,
                    width="large",
                ),
                "상품링크": st.column_config.LinkColumn(
                    "상품링크",
                    display_text="🔗 열기",
                    width="small",
                ),
            },
            disabled=[c for c in disp_cols if c != "판단"],
            use_container_width=True,
            height=520,
            hide_index=True,
            key="judgment_editor",
        )

        # ── 변경사항 감지 및 저장 ─────────────────────────────────────────────
        # 복합키(캠페인::검색어)로 중복 검색어 문제 방지, override는 검색어 단위로 저장
        view["_key"] = view["캠페인"] + "::" + view["검색어"]
        edited["_key"] = view["_key"].values

        orig = view.set_index("_key")["판단"].to_dict()
        edit = edited.set_index("_key")["판단"].to_dict()
        changed_items = {k: v for k, v in edit.items() if orig.get(k) != v}

        if changed_items:
            for composite_key, judgment in changed_items.items():
                kw = composite_key.split("::", 1)[1]
                overrides[kw] = judgment
            save_overrides(overrides)
            st.success(f"✅ {len(changed_items)}개 항목 저장됨")
            st.rerun()

        # 오버라이드 초기화 버튼
        if overrides:
            if st.button("🔄 판단 수동변경 전체 초기화", type="secondary"):
                save_overrides({})
                st.rerun()

        st.caption("판단 셀을 클릭해 값을 변경할 수 있습니다. ASIN 행은 링크로 상품 확인 후 판단을 업데이트하세요.")

        # ── 캠페인별 요약 ─────────────────────────────────────────────────────
        st.divider()
        st.subheader("캠페인별 요약")
        summary = (
            cands.groupby("캠페인")
            .agg(
                제외후보=("검색어", "count"),
                예상절감액=("비용", "sum"),
                정확히일치=("제외타입", lambda x: (x == "정확히 일치").sum()),
                구문일치=("제외타입",   lambda x: (x == "구문 일치").sum()),
                ASIN확인필요=("판단",   lambda x: (x == "🔗 확인 필요 (ASIN)").sum()),
            )
            .reset_index()
        )
        summary["예상절감액"] = summary["예상절감액"].apply(lambda v: f"¥{v:,.0f}")
        st.dataframe(summary, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 3 — 히스토리 조회 · 내보내기
# ══════════════════════════════════════════════════════════════════════════════
with tab3:

    # ── 현재 세션 저장 ────────────────────────────────────────────────────────
    if st.session_state.campaigns:
        overrides = load_overrides()
        all_df    = pd.concat(st.session_state.campaigns.values(), ignore_index=True)
        cands_t3  = calc_exclusions(all_df, overrides)
        cands_t3  = attach_translations(cands_t3)
        today_str = datetime.date.today().strftime("%Y-%m-%d")

        # 오늘 날짜로 이미 저장된 데이터 확인
        with get_db() as _conn:
            already = pd.read_sql("SELECT COUNT(*) as cnt FROM history WHERE 날짜=?", _conn, params=[today_str]).iloc[0]["cnt"]

        col_save, col_info = st.columns([2, 3])
        if already > 0:
            col_info.warning(f"오늘({today_str}) 이미 {already}개 저장됨 — 재저장 시 중복")
        if col_save.button("💾 현재 검토 결과 DB 저장", type="primary"):
            save_to_db(cands_t3, today_str)
            st.success(f"✅ {len(cands_t3)}개 항목을 저장했습니다. (날짜: {today_str})")

    st.divider()

    # ── 히스토리 조회 ─────────────────────────────────────────────────────────
    st.subheader("📅 히스토리 조회")

    meta = get_history_meta()

    if not meta["dates"]:
        st.info("저장된 히스토리가 없습니다. 위 버튼으로 먼저 저장해주세요.")
    else:
        today = datetime.date.today()

        # 필터
        fc1, fc2, fc3, fc4 = st.columns([2, 2, 2, 2])

        date_range = fc1.date_input(
            "날짜 범위",
            value=(today, today),
            min_value=datetime.date.fromisoformat(meta["dates"][-1]),
            max_value=datetime.date.fromisoformat(meta["dates"][0]),
        )

        sel_camps = fc2.multiselect(
            "캠페인", meta["campaigns"], default=meta["campaigns"], key="tab3_camp"
        )
        sel_judge = fc3.multiselect(
            "판단", JUDGMENT_OPTIONS, default=JUDGMENT_OPTIONS, key="tab3_judge"
        )

        # 날짜 범위 처리 (date_input이 단일값 반환할 수 있음)
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            d_from, d_to = str(date_range[0]), str(date_range[1])
        else:
            d_from = d_to = str(date_range)

        hist_df = query_history(d_from, d_to, sel_camps, sel_judge)

        fc4.metric("조회 결과", f"{len(hist_df)}개")

        if hist_df.empty:
            st.info("조건에 맞는 데이터가 없습니다.")
        else:
            view_cols = ["날짜", "캠페인", "검색어", "발음", "의미", "매치타입", "비용", "구매수", "매출", "ACOS", "제외타입", "판단"]
            st.dataframe(hist_df[view_cols], use_container_width=True, height=480, hide_index=True)

            st.divider()

            # ── 엑셀 내보내기 ─────────────────────────────────────────────────
            out = BytesIO()
            with pd.ExcelWriter(out, engine="openpyxl") as writer:
                hist_df[view_cols].to_excel(writer, index=False, sheet_name="히스토리")
            st.download_button(
                label="📥 조회 결과 엑셀 내보내기",
                data=out.getvalue(),
                file_name=f"amazon_jp_히스토리_{d_from}_{d_to}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
            )
