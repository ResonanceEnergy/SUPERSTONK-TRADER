import os
import zipfile
import argparse
from datetime import datetime

EXCLUDES = {'.env', '.venv', '__pycache__', '.git', 'exports'}

def should_skip(rel_path: str):
    parts = rel_path.replace('\\', '/').split('/')
    if any(p in EXCLUDES for p in parts):
        return True
    if rel_path.lower().endswith(('.pyc', '.pyo')):
        return True
    return False


def main():
    ap = argparse.ArgumentParser(description='Export project to ZIP (excludes secrets and venv).')
    ap.add_argument('--root', default='.', help='Project root folder')
    ap.add_argument('--name', default='superstonk_dd_autopilot', help='ZIP base name')
    ap.add_argument('--outdir', default='exports', help='Output folder')
    args = ap.parse_args()

    root = os.path.abspath(args.root)
    outdir = os.path.join(root, args.outdir)
    os.makedirs(outdir, exist_ok=True)

    ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    outpath = os.path.join(outdir, f'{args.name}_{ts}.zip')

    with zipfile.ZipFile(outpath, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for base, dirs, files in os.walk(root):
            rel_base = os.path.relpath(base, root)
            if rel_base == '.':
                rel_base = ''

            keep = []
            for d in dirs:
                rel_d = os.path.join(rel_base, d) if rel_base else d
                if should_skip(rel_d):
                    continue
                keep.append(d)
            dirs[:] = keep

            for f in files:
                if f == '.env':
                    continue
                rel_f = os.path.join(rel_base, f) if rel_base else f
                if should_skip(rel_f):
                    continue
                zf.write(os.path.join(base, f), rel_f)

    print('ZIP created:')
    print(' -', outpath)


if __name__ == '__main__':
    main()
