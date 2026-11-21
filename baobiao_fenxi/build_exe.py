"""把 baobiao_fenxi 打包成 Windows 可执行文件（PyInstaller 封装）。

用法（在任意目录执行均可，脚本以自身位置定位项目）：
    python build_exe.py                     # 打包单文件 exe，并把运行期文件放到发布目录
    python build_exe.py --onedir            # 打包成目录形式（启动更快，便于排查问题）
    python build_exe.py --console           # 保留控制台窗口，便于查看异常输出
    python build_exe.py --name baobiao      # 自定义 exe 名称
    python build_exe.py --no-install        # 不自动安装 PyInstaller，缺失时直接报错

说明：
    - 本脚本只使用标准库；PyInstaller 是构建期依赖，缺失时会提示并自动安装。
    - 生成的 exe 内置 Python 运行时与 Qt、akshare、pandas、numpy，目标机器无需安装 Python。
    - 界面通过 QProcess 调用 baobiaofenxi.py，而打包后 sys.executable 变成 exe 自身，
      且界面用脚本所在目录定位配置与输出。因此构建时生成一份临时入口（写在系统临时目录，
      不改动项目源码），让同一个 exe 具备两种模式：
        正常启动：把界面目录常量改到 exe 同目录后调用 baobiao_ui.main()；
        带 "-u xxx.py -f ... -o ..." 启动：在 exe 内直接执行 baobiaofenxi.py。
    - 配置、脚本、输出均需运行期读写，故不打进 exe 内部，而是放到 exe 同目录：
      baobiaofenxi.py、config.ini、setting.ini、output。发布时请整目录拷贝。
"""

import argparse
import configparser
import importlib.util
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
ENTRY_SCRIPT = PROJECT_DIR / 'baobiao_ui.py'
CLI_SCRIPT = PROJECT_DIR / 'baobiaofenxi.py'
DIST_DIR = PROJECT_DIR / 'dist'
BUILD_DIR = PROJECT_DIR / 'build'

# 运行期必须具备的第三方库（缺任意一个打包出来的程序都跑不起来）
RUNTIME_DEPS = ['akshare', 'numpy', 'pandas']
QT_BINDINGS = ['PyQt5', 'PySide6']

# 构建完成后放到 exe 同目录的文件（脚本要被界面检测，配置需要可改）
COPY_TO_DIST = ['baobiaofenxi.py', 'config.ini']
# 运行期写入的目录：只创建，不复制已有 csv，避免把旧数据带进发布包
ENSURE_DIRS = ['output']

# 临时入口代码：仅用于本次构建，不写入项目目录
LAUNCHER_CODE = '''"""PyInstaller 临时入口：兼容界面以子进程方式调用分析脚本。"""
import io
import os
import runpy
import sys
from pathlib import Path

RUNTIME_DIR = Path(sys.executable).resolve().parent
CLI_SCRIPT = RUNTIME_DIR / 'baobiaofenxi.py'

# 界面用 sys.executable（即本 exe）启动分析脚本，参数为 -u 脚本 -f 配置 -o 输出
is_worker = len(sys.argv) > 2 and sys.argv[1] == '-u' and sys.argv[2].endswith('.py')

if is_worker:
    # windowed 模式下 PyInstaller 会把标准输出置为空写入器，需重新绑定到 QProcess 的管道
    for fd, name in ((1, 'stdout'), (2, 'stderr')):
        try:
            stream = io.TextIOWrapper(
                os.fdopen(os.dup(fd), 'wb', buffering=0),
                encoding='utf-8', errors='replace', line_buffering=True,
            )
            setattr(sys, name, stream)
        except OSError:
            pass
    sys.argv = [str(CLI_SCRIPT)] + sys.argv[3:]
    runpy.run_path(str(CLI_SCRIPT), run_name='__main__')
    sys.stdout.flush()
    sys.stderr.flush()
else:
    import baobiao_ui

    # 界面按 __file__ 定位资源，打包后统一改到 exe 同目录（配置与输出需要可写）
    baobiao_ui.SCRIPT_DIR = RUNTIME_DIR
    baobiao_ui.SCRIPT_PATH = CLI_SCRIPT
    baobiao_ui.DEFAULT_CONFIG = RUNTIME_DIR / 'config.ini'
    baobiao_ui.DEFAULT_OUTPUT = RUNTIME_DIR / 'output'
    baobiao_ui.SETTINGS_PATH = RUNTIME_DIR / 'setting.ini'
    baobiao_ui.main()
'''


def _has_module(name):
    """判断当前解释器是否可导入指定模块（不真正 import，避免拖慢启动）。"""
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def _check_runtime_deps():
    """检查运行期依赖，返回实际使用的 Qt 绑定名，缺失的库直接报错退出。"""
    missing = [name for name in RUNTIME_DEPS if not _has_module(name)]
    qt_used = next((name for name in QT_BINDINGS if _has_module(name)), None)
    if qt_used is None:
        missing.append('PyQt5 或 PySide6')
    if missing:
        print('[错误] 缺少运行期依赖：' + '、'.join(missing))
        print('       请先执行： pip install akshare numpy pandas PyQt5')
        raise SystemExit(1)
    print('[信息] Qt 绑定：%s' % qt_used)
    return qt_used


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


def _build_command(args, qt_used, launcher):
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

    # 分析脚本由 runpy 动态执行，显式声明为隐藏模块以收集 akshare/pandas/numpy
    cmd += ['--hidden-import', 'baobiaofenxi']
    # akshare 大量使用函数内延迟导入，必须整体收集子模块与数据文件
    cmd += ['--collect-submodules', 'akshare', '--collect-data', 'akshare']

    # 排除未使用的另一套 Qt 绑定，避免体积翻倍和插件冲突
    for binding in QT_BINDINGS:
        if binding != qt_used:
            cmd += ['--exclude-module', binding]

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


def _copy_settings(target_dir):
    """复制界面设置，并把配置/输出路径改到发布目录，避免 exe 写回源码目录。"""
    source = PROJECT_DIR / 'setting.ini'
    if not source.exists():
        print('[警告] 未找到 %s，界面使用默认设置' % source)
        return
    parser = configparser.ConfigParser(interpolation=None)
    try:
        parser.read(source, encoding='utf-8')
    except (OSError, configparser.Error) as exc:
        print('[警告] 读取 %s 失败（%s），界面使用默认设置' % (source, exc))
        return
    if parser.has_section('ui'):
        parser.set('ui', 'config', str(target_dir / 'config.ini'))
        parser.set('ui', 'output', str(target_dir / 'output'))
    try:
        with open(target_dir / 'setting.ini', 'w', encoding='utf-8') as file:
            parser.write(file)
    except OSError as exc:
        print('[警告] 写入 setting.ini 失败（%s），界面使用默认设置' % exc)


def _copy_runtime_files(args):
    """把运行期需要读写的脚本、配置和目录放到 exe 同目录。"""
    target_dir = _exe_dir(args)
    for name in COPY_TO_DIST:
        source = PROJECT_DIR / name
        if not source.exists():
            print('[警告] 未找到 %s，已跳过复制' % source)
            continue
        shutil.copyfile(source, target_dir / name)
    for name in ENSURE_DIRS:
        (target_dir / name).mkdir(parents=True, exist_ok=True)
    _copy_settings(target_dir)
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
    print('       请整目录拷贝（含 baobiaofenxi.py、config.ini、setting.ini、output）。')


def main():
    parser = argparse.ArgumentParser(description='打包 baobiao_fenxi 为 Windows exe')
    parser.add_argument('-n', '--name', default='baobiao_fenxi', help='exe 名称（默认 baobiao_fenxi）')
    parser.add_argument('--onedir', action='store_true', help='打包成目录形式（默认为单文件）')
    parser.add_argument('--console', action='store_true', help='保留控制台窗口，便于排查异常')
    parser.add_argument('--icon', default='', help='exe 图标文件路径（.ico）')
    parser.add_argument('--no-install', action='store_true', help='不自动安装 PyInstaller')
    args = parser.parse_args()

    for script in (ENTRY_SCRIPT, CLI_SCRIPT):
        if not script.exists():
            print('[错误] 未找到脚本：%s' % script)
            raise SystemExit(1)

    qt_used = _check_runtime_deps()
    _ensure_pyinstaller(auto_install=not args.no_install)

    # 入口写在系统临时目录，避免 --clean 清理 build 目录时删掉它
    with tempfile.TemporaryDirectory(prefix='baobiao_exe_') as tmp_dir:
        launcher = Path(tmp_dir) / 'pyi_entry.py'
        launcher.write_text(LAUNCHER_CODE, encoding='utf-8')
        cmd = _build_command(args, qt_used, launcher)
        print('[信息] 开始打包：%s' % ' '.join(cmd))
        result = subprocess.run(cmd, cwd=str(PROJECT_DIR))
    if result.returncode != 0:
        print('[错误] PyInstaller 打包失败，返回码：%s' % result.returncode)
        raise SystemExit(result.returncode)

    _copy_runtime_files(args)
    _report(args)


if __name__ == '__main__':
    main()
