# matcher/models.py

from dataclasses import dataclass, field
from typing import Any, List, Optional
import pandas as pd


@dataclass
class ColumnSchema:
    # 컬럼 이름
    name: str
    # 내부 분석용 타입 (STRING / NUMERIC / DATE 등, normalize된 값)
    data_type: str
    # 실제 테이블/엑셀 스키마에 적힌 원본 타입 (예: STRING, DATETIME, TIMESTAMP, NUMBER 등)
    raw_data_type: Optional[str] = None

    # 추가 프로파일링 정보
    nullable: Optional[bool] = None
    distinct_count: Optional[int] = None
    null_ratio: Optional[float] = None
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class TableProfile:
    table_name: str
    columns: List[ColumnSchema]
    sample_df: pd.DataFrame
    row_count: int
