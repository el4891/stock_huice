"""把 tools/choujiang 打包成 Windows 可执行文件（PyInstaller 封装）。

用法（在任意目录执行均可，脚本以自身位置定位项目）：
    python build_exe.py                     # 打包单文件 exe
    python build_exe.py --onedir            # 打包成目录形式（启动更快，便于排查问题）
    python build_exe.py --console           # 保留控制台窗口，便于查看异常输出
    python build_exe.py --name choujiang    # 自定义 exe 名称
    python build_exe.py --no-install        # 不自动安装 PyInstaller，缺失时直接报错

说明：
    - 本脚本只使用标准库；PyInstaller 是构建期依赖，缺失时会提示并自动安装。
    - 程序仅依赖 PySide6，界面无需调用外部子进程。
    - 配置在运行时读写，故 settings.json 不打进 exe，而是放到 exe 同目录。
      通过临时入口把 config_store.CONFIG_PATH 改到 exe 同目录，避免写入打包临时目录。
"""

import argparse
import importlib.util
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
ENTRY_SCRIPT = PROJECT_DIR / 'main.py'
DIST_DIR = PROJECT_DIR / 'dist'
BUILD_DIR = PROJECT_DIR / 'build'

# 运行期需要安装的第三方库（本程序仅使用 PySide6）
RUNTIME_DEPS = ['PySide6']

# 构建完成后放到 exe 同目录的文件（配置需要运行期读写）
COPY_TO_DIST = ['settings.json']

# 临时入口代码：仅用于本次构建，不写入项目目录
LAUNCHER_CODE = '''"""PyInstaller 临时入口：把配置路径改到 exe 同目录。"""
import sys
from pathlib import Path

RUNTIME_DIR = Path(sys.executable).resolve().parent

# 先引入配置模块并改写路径，后续模块共用同一个模块对象，读写都落在 exe 同目录
import config_store
config_store.CONFIG_PATH = RUNTIME_DIR / 'settings.json'

import main
main.main()
'''


def _has_module(name):
    """判断当前解释器是否可导入指定模块（不真正 import，避免拖慢启动）。"""
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def _check_runtime_deps():
    """检查运行期依赖，缺失时报错退出。"""
    missing = [name for name in RUNTIME_DEPS if not _has_module(name)]
    if missing:
        print('[错误] 缺少运行期依赖：' + '、'.join(missing))
        print('       请先执行： pip install "PySide6>=6.8,<7"')
        raise SystemExit(1)


def _ensure_pyinstaller(auto_install):
    """确保 PyInstaller 可用；auto_install 为 True 时用 pip 自动安装。"""
    if _has_module('PyInstaller'):
        return
    if not auto_install:
        print('[错误] 未安装 PyInstaller，请执行： pip install pyinstaller')
        raise SystemExit(1)
    print('[信息] 未检测到 PyInstaller，正在安装 ...')
    result = subprocess.run(
        [sys.executable, '-m', 'pip', 'install', 'pyinstaller'],
        cwd=str(PROJECT_DIR),
    )
    if result.returncode != 0:
        print('[错误] PyInstaller 安装失败，请手动执行： pip install pyinstaller')
        raise SystemExit(result.returncode)


def _build_command(args, launcher):
    """拼装 PyInstaller 命令行参数。"""
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--noconfirm',
        '--clean',
        '--onefile' if not args.onedir else '--onedir',
        '--console' if args.console else '--windowed',
        '--name', args.name,
        '--distpath', str(DIST_DIR),
        '--workpath', str(BUILD_DIR),
        '--specpath', str(BUILD_DIR),
        '--paths', str(PROJECT_DIR),
    ]
    if args.icon:
        icon_path = Path(args.icon).expanduser()
        if icon_path.exists():
            cmd += ['--icon', str(icon_path.resolve())]
        else:
            print('[警告] 未找到图标文件：%s，已跳过' % icon_path)
    cmd.append(str(launcher))
    return cmd


def _exe_dir(args):
    """返回 exe 实际输出目录：单文件在 dist 下，目录形式在 dist/<name> 下。"""
    return DIST_DIR if not args.onedir else DIST_DIR / args.name


def _copy_runtime_files(args):
    """把运行期需要读写的配置放到 exe 同目录。"""
    target_dir = _exe_dir(args)
    for name in COPY_TO_DIST:
        source = PROJECT_DIR / name
        if not source.exists():
            print('[警告] 未找到 %s，已跳过复制' % source)
            continue
        shutil.copyfile(source, target_dir / name)
    return target_dir


def _report(args):
    """打印产物位置与体积，方便直接双击运行。"""
    exe_path = _exe_dir(args) / ('%s.exe' % args.name)
    if not exe_path.exists():
        print('[错误] 未找到产物：%s' % exe_path)
        raise SystemExit(1)
    size_mb = exe_path.stat().st_size / 1024 / 1024
    print('')
    print('[完成] 可执行文件：%s（%.1f MB）' % (exe_path, size_mb))
    print('[完成] 发布目录：%s' % _exe_dir(args))
    print('       请整目录拷贝（含 settings.json），否则首次运行会生成默认配置。')


def main():
    parser = argparse.ArgumentParser(description='打包 choujiang 为 Windows exe')
    parser.add_argument('-n', '--name', default='choujiang', help='exe 名称（默认 choujiang）')
    parser.add_argument('--onedir', action='store_true', help='打包成目录形式（默认为单文件）')
    parser.add_argument('--console', action='store_true', help='保留控制台窗口，便于排查异常')
    parser.add_argument('--icon', default='', help='exe 图标文件路径（.ico）')
    parser.add_argument('--no-install', action='store_true', help='不自动安装 PyInstaller')
    args = parser.parse_args()

    if not ENTRY_SCRIPT.exists():
        print('[错误] 未找到脚本：%s' % ENTRY_SCRIPT)
        raise SystemExit(1)

    _check_runtime_deps()
    _ensure_pyinstaller(auto_install=not args.no_install)

    # 入口写在系统临时目录，避免 --clean 清理 build 目录时删掉它
    with tempfile.TemporaryDirectory(prefix='choujiang_exe_') as tmp_dir:
        launcher = Path(tmp_dir) / 'pyi_entry.py'
        launcher.write_text(LAUNCHER_CODE, encoding='utf-8')
        cmd = _build_command(args, launcher)
        print('[信息] 开始打包：%s' % ' '.join(cmd))
        result = subprocess.run(cmd, cwd=str(PROJECT_DIR))
    if result.returncode != 0:
        print('[错误] PyInstaller 打包失败，返回码：%s' % result.returncode)
        raise SystemExit(result.returncode)

    _copy_runtime_files(args)
    _report(args)


if __name__ == '__main__':
    main()
