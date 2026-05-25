"""CLI 入口：A股统一数据源。"""

import argparse
import json
import sys
from typing import Any, Dict

from tstock.logging_config import setup_logging
from tstock_data_source.snapshot import fetch_stock_snapshot
from tstock_data_source.batch import fetch_batch, get_scope_codes


def main():
    p = argparse.ArgumentParser(description="A股统一数据源")
    p.add_argument("--verbose", action="store_true", help="显示详细日志")
    p.add_argument("--debug", action="store_true", help="显示调试日志")
    p.add_argument("--code", type=str, help="单只股票代码")
    p.add_argument("--codes", type=str, help="多只股票代码，逗号分隔")
    p.add_argument("--scope", type=str, help="指数范围: hs300/zz500/zz1000/cyb/kcb/all")
    p.add_argument("--data-type", type=str, default="core", choices=["core", "financial", "all"])
    p.add_argument("--years", type=int, default=3)
    p.add_argument("--no-cache", action="store_true")
    p.add_argument("--output", type=str, help="单次输出文件")
    p.add_argument("--batch-output", type=str, help="批量输出文件")

    args = p.parse_args()
    setup_logging("DEBUG" if args.debug else ("INFO" if args.verbose else "WARNING"))
    use_cache = not args.no_cache

    result: Dict[str, Any]

    if args.code:
        result = fetch_stock_snapshot(args.code, data_type=args.data_type, years=args.years, use_cache=use_cache)
    elif args.codes:
        codes = [x.strip() for x in args.codes.split(",") if x.strip()]
        result = fetch_batch(codes, args.data_type, args.years, use_cache)
    elif args.scope:
        codes = get_scope_codes(args.scope)
        result = {
            "scope": args.scope,
            "count": len(codes),
            "codes": codes
        }
    else:
        print("请提供 --code / --codes / --scope 之一")
        sys.exit(1)

    out = json.dumps(result, ensure_ascii=False, indent=2, default=str)

    file_path = args.batch_output if args.batch_output else args.output
    if file_path:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"已写入: {file_path}")
    else:
        print(out)


if __name__ == "__main__":
    main()
