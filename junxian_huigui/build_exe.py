"""把 junxian_huigui 打包成 Windows 可执行文件（PyInstaller 封装）。

用法（在任意目录执行均可，脚本以自身位置定位项目）：
    python build_exe.py                     # 打包单文件 exe，并把 settings.ini、data_cache 复制到发布目录
    python build_exe.py --onedir            # 打包成目录形式（启动更快，便于排查问题）
    python build_exe.py --console           # 保留控制台窗口，便于查看异常输出
    python build_exe.py --name ma_backtest  # 自定义 exe 名称
    python build_exe.py --no-install        # 不自动安装 PyInstaller，缺失时直接报错

说明：
    - 本脚本只使用标准库；PyInstaller 是打包工具（构建期依赖），缺失时会提示并自动安装。
    - 生成的 exe 内置 Python 运行时与 PyQt5/PySide6、pandas、backtrader、akshare，
      目标机器无需安装 Python，也无需安装任何第三方库。
    - assets 与默认 settings.ini 会打进 exe；settings.ini、data_cache 另外复制到 exe 同目录，
      因为这两项在运行期需要写入（配置保存、行情缓存），写在临时解包目录会丢失。
"""

import argparse
import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
ENTRY_SCRIPT = PROJECT_DIR / 'junxian_huigui_ui.py'
DIST_DIR = PROJECT_DIR / 'dist'
BUILD_DIR = PROJECT_DIR / 'build'

# 打进 exe 内部的只读资源：(源路径, exe 内的目标目录)
BUNDLED_ITEMS = [
    (PROJECT_DIR / 'assets', 'assets'),
    (PROJECT_DIR / 'settings.ini', '.'),
]

# 运行期必须具备的第三方库（缺任意一个打包出来的程序都跑不起来）
RUNTIME_DEPS = ['pandas', 'backtrader', 'akshare']
QT_BINDINGS = ['PyQt5', 'PySide6']

# 构建完成后复制到 exe 同目录的可写文件/目录（运行期会被写入，不能只放在 exe 内）
COPY_TO_DIST = ['settings.ini', 'data_cache']


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
        print('       请先执行： pip install -r requirements.txt')
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


def _data_separator():
    """PyInstaller --add-data 的源/目标分隔符：Windows 用分号，其它平台用冒号。"""
    return ';' if sys.platform.startswith('win') else ':'


def _build_command(args, qt_used):
    """拼装 PyInstaller 命令行参数。"""
    sep = _data_separator()
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
    ]

    # 只读资源打入 exe
    for source, target in BUNDLED_ITEMS:
        if source.exists():
            cmd += ['--add-data', '%s%s%s' % (source, sep, target)]
        else:
            print('[警告] 未找到资源：%s，已跳过' % source)

    # akshare 大量使用函数内延迟导入，必须整体收集子模块与数据文件，否则打包后运行时报缺模块
    cmd += ['--collect-submodules', 'akshare', '--collect-data', 'akshare']
    # backtrader 的部分指标/过滤器按字符串动态加载，一并收集
    cmd += ['--collect-submodules', 'backtrader']

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

    cmd.append(str(ENTRY_SCRIPT))
    return cmd


def _exe_dir(args):
    """返回 exe 实际输出目录：单文件在 dist 下，目录形式在 dist/<name> 下。"""
    return DIST_DIR if not args.onedir else DIST_DIR / args.name


def _copy_writable_files(args):
    """把运行期需要写入的配置与缓存复制到 exe 同目录。"""
    target_dir = _exe_dir(args)
    for name in COPY_TO_DIST:
        source = PROJECT_DIR / name
        if not source.exists():
            print('[警告] 未找到 %s，已跳过复制' % source)
            continue
        target = target_dir / name
        # 已存在的同名文件/目录直接覆盖合并，保证多次打包结果一致
        if source.is_dir():
            shutil.copytree(source, target, dirs_exist_ok=True)
        else:
            shutil.copyfile(source, target)
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
    print('       该目录可直接拷贝到其它 Windows 机器运行，无需安装 Python 和第三方库。')


def main():
    parser = argparse.ArgumentParser(description='打包 junxian_huigui 为 Windows exe')
    parser.add_argument('-n', '--name', default='junxian_huigui', help='exe 名称（默认 junxian_huigui）')
    parser.add_argument('--onedir', action='store_true', help='打包成目录形式（默认为单文件）')
    parser.add_argument('--console', action='store_true', help='保留控制台窗口，便于排查异常')
    parser.add_argument('--icon', default='', help='exe 图标文件路径（.ico）')
    parser.add_argument('--no-install', action='store_true', help='不自动安装 PyInstaller')
    args = parser.parse_args()

    if not ENTRY_SCRIPT.exists():
        print('[错误] 未找到入口脚本：%s' % ENTRY_SCRIPT)
        raise SystemExit(1)

    qt_used = _check_runtime_deps()
    _ensure_pyinstaller(auto_install=not args.no_install)

    cmd = _build_command(args, qt_used)
    print('[信息] 开始打包：%s' % ' '.join(cmd))
    result = subprocess.run(cmd, cwd=str(PROJECT_DIR))
    if result.returncode != 0:
        print('[错误] PyInstaller 打包失败，返回码：%s' % result.returncode)
        raise SystemExit(result.returncode)

    _copy_writable_files(args)
    _report(args)


if __name__ == '__main__':
    main()
