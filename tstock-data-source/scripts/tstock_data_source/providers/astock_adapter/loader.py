"""
a-stock-data 动态适配器与代码载入器
===================================
通过 AST 语法分析，从上游 ~/Projects/upstream/a-stock-data/SKILL.md 中提取
纯净的函数定义、类定义、导入声明及全局常量，剔除文档中用于演示的顶层调用与示例代码。

优势：
1. 保持 upstream 为只读（无需修改其代码），支持随时 git pull 升级。
2. 避免了包发布、依赖冲突与路径硬编码。
3. 线程安全的单例缓存，毫秒级就绪。
"""

import ast
import logging
import re
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import config

logger = logging.getLogger(__name__)

_namespace: Optional[Dict[str, Any]] = None
_lock = threading.Lock()


def _get_root_name(node: ast.AST) -> Optional[str]:
    while isinstance(node, ast.Attribute):
        node = node.value
    if isinstance(node, ast.Name):
        return node.id
    return None


def _is_target_kept(target: ast.AST) -> bool:
    name = _get_root_name(target)
    if name and (name.isupper() or name.startswith("_")):
        return True
    return False


def _filter_node(node: ast.AST) -> List[ast.AST]:
    # 保留所有模块导入、函数定义、异步函数定义、类定义
    if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return [node]

    # 保留大写常量与私有配置赋值（如 UA, SH_INDEX, EM_SESSION, _TDX_SERVERS 等）
    if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if any(_is_target_kept(t) for t in targets):
            return [node]

    # 保留对全局大写/私有对象的配置调用（如 EM_SESSION.headers.update(...) / EM_SESSION.mount(...)）
    elif isinstance(node, ast.Expr):
        if isinstance(node.value, ast.Call):
            name = _get_root_name(node.value.func)
            if name and (name.isupper() or name.startswith("_")):
                return [node]

    # 递归过滤 Try 块（用于安全初始化适配器或导入 fallback）
    elif isinstance(node, ast.Try):
        new_body = []
        for n in node.body:
            new_body.extend(_filter_node(n))
        if new_body:
            node.body = new_body
            return [node]

    # 过滤掉所有小写的示例赋值（如 client = ...）、顶层 print()、示例循环等
    return []


def load_astock_namespace(skill_path: Optional[Path] = None, force_reload: bool = False) -> Dict[str, Any]:
    """从 SKILL.md 解析并载入 a-stock-data 的命名空间。"""
    global _namespace

    with _lock:
        if _namespace is not None and not force_reload:
            return _namespace

        path = skill_path or (config.ASTOCK_DATA_ROOT / "SKILL.md")
        if not path.exists():
            raise FileNotFoundError(f"a-stock-data SKILL.md not found at {path}")

        content = path.read_text(encoding="utf-8")
        blocks = re.findall(r"```python\n(.*?)\n```", content, re.DOTALL)

        clean_nodes = []
        for i, block in enumerate(blocks):
            try:
                tree = ast.parse(block, filename=f"block_{i}")
                for node in tree.body:
                    clean_nodes.extend(_filter_node(node))
            except SyntaxError as e:
                logger.warning("SyntaxError parsing block %d in %s: %s", i, path, e)

        module = ast.Module(body=clean_nodes, type_ignores=[])
        ast.fix_missing_locations(module)
        code_obj = compile(module, str(path), "exec")

        ns: Dict[str, Any] = {}
        exec(code_obj, ns)

        _namespace = ns
        logger.info("Successfully loaded %d symbols from %s", len(ns), path)
        return _namespace


def get_astock_function(func_name: str) -> Optional[Callable]:
    """安全获取 a-stock-data 中的指定函数。若不存在则返回 None。"""
    try:
        ns = load_astock_namespace()
        func = ns.get(func_name)
        if callable(func):
            return func
        return None
    except Exception as e:
        logger.error("Failed to get astock function '%s': %s", func_name, e)
        return None


def call_astock(func_name: str, *args, **kwargs) -> Any:
    """直接调用 a-stock-data 中的指定函数。"""
    func = get_astock_function(func_name)
    if func is None:
        raise AttributeError(f"Function '{func_name}' not found in a-stock-data")
    return func(*args, **kwargs)
