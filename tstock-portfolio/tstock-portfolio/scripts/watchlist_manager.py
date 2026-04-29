#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime
from pathlib import Path

# 自选股数据库路径，可通过环境变量 OPENCLAW_WATCHLIST_DB 覆盖
# 默认位于 workspace-fiona/memory/watchlist.json
_watchlist_default = str(
    Path(__file__).resolve().parent.parent.parent.parent / "memory" / "watchlist.json"
)
DB = os.environ.get('OPENCLAW_WATCHLIST_DB', _watchlist_default)


def load_db():
    if os.path.exists(DB):
        with open(DB, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'items': []}


def save_db(data):
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    with open(DB, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def add(code, name='', group='default', note=''):
    data = load_db()
    items = data['items']
    code = code.strip()
    for x in items:
        if x['code'] == code:
            x.update({'name': name or x.get('name', ''), 'group': group or x.get('group', 'default'), 'note': note or x.get('note', ''), 'updated_at': datetime.now().isoformat()})
            save_db(data)
            return {'status': 'updated', 'item': x}
    item = {
        'code': code,
        'name': name,
        'group': group,
        'note': note,
        'created_at': datetime.now().isoformat(),
        'updated_at': datetime.now().isoformat()
    }
    items.append(item)
    save_db(data)
    return {'status': 'added', 'item': item}


def remove(code):
    data = load_db()
    before = len(data['items'])
    data['items'] = [x for x in data['items'] if x['code'] != code]
    save_db(data)
    return {'removed': before - len(data['items'])}


def list_items(group=None):
    data = load_db()
    items = data['items']
    if group:
        items = [x for x in items if x.get('group') == group]
    return items


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='cmd', required=True)

    p_add = sub.add_parser('add')
    p_add.add_argument('--code', required=True)
    p_add.add_argument('--name', default='')
    p_add.add_argument('--group', default='default')
    p_add.add_argument('--note', default='')

    p_rm = sub.add_parser('remove')
    p_rm.add_argument('--code', required=True)

    p_ls = sub.add_parser('list')
    p_ls.add_argument('--group')

    args = p.parse_args()

    if args.cmd == 'add':
        out = add(args.code, args.name, args.group, args.note)
    elif args.cmd == 'remove':
        out = remove(args.code)
    else:
        out = list_items(args.group)

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
